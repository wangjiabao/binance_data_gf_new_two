package logic

import (
	"binance_data_gf/internal/model/do"
	"binance_data_gf/internal/model/entity"
	"binance_data_gf/internal/service"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gateio/gateapi-go/v6"
	"github.com/gogf/gf/v2/container/gmap"
	"github.com/gogf/gf/v2/container/gtype"
	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/grpool"
	"github.com/gogf/gf/v2/os/gtime"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type (
	sBinanceTraderHistory struct {
		pool *grpool.Pool
	}
)

func init() {
	service.RegisterBinanceTraderHistory(New())
}

func New() *sBinanceTraderHistory {
	return &sBinanceTraderHistory{
		grpool.New(), // 这里是请求协程池子，可以配合着可并行请求binance的限制使用，来限制最大共存数，后续jobs都将排队，考虑到上层的定时任务
	}
}

func IsEqual(f1, f2 float64) bool {
	if f1 > f2 {
		return f1-f2 < 0.000000001
	} else {
		return f2-f1 < 0.000000001
	}
}

func lessThanOrEqualZero(a, b float64, epsilon float64) bool {
	return a-b < epsilon || math.Abs(a-b) < epsilon
}

var (
	globalTraderNum = uint64(4150465500682762240) // todo 改 3887627985594221568

	baseMoneyGuiTu      = gtype.NewFloat64() // 带单人保证金
	baseMoneyTotalGuiTu = gtype.NewFloat64() // 跟单规模出去带单人保证金
	baseMoneyUserAllMap = gmap.NewIntAnyMap(true)

	globalUsers = gmap.New(true)

	// 仓位
	binancePositionMap = make(map[string]*entity.TraderPosition, 0)

	symbolsMap = gmap.NewStrAnyMap(true)
	//symbolsMapGate = gmap.NewStrAnyMap(true)

	locKOrder     = gmap.NewStrAnyMap(true)
	locKOrderTime = gmap.NewStrAnyMap(true)
	exMap         = gmap.NewStrAnyMap(true)

	running = true
)

// GetGlobalInfo 获取全局测试数据
func (s *sBinanceTraderHistory) GetGlobalInfo(ctx context.Context) {
}

type SymbolGate struct {
	Symbol           string  `json:"symbol"            ` //
	QuantoMultiplier float64 `json:"quantityPrecision" ` //
	OrderPriceRound  int
}

// UpdateCoinInfo 初始化信息
func (s *sBinanceTraderHistory) UpdateCoinInfo(ctx context.Context) bool {
	//// 获取代币信息
	//var (
	//	err     error
	//	symbols []*entity.LhCoinSymbol
	//)
	//err = g.Model("lh_coin_symbol").Ctx(ctx).Scan(&symbols)
	//if nil != err || 0 >= len(symbols) {
	//	fmt.Println("龟兔，初始化，币种，数据库查询错误：", err)
	//	return false
	//}
	//// 处理
	//for _, vSymbols := range symbols {
	//	symbolsMap.Set(vSymbols.Symbol+"USDT", vSymbols)
	//}
	//
	//return true

	// 获取代币信息
	var (
		err               error
		binanceSymbolInfo []*BinanceSymbolInfo
	)
	binanceSymbolInfo, err = getBinanceFuturesPairs()
	if nil != err {
		log.Println("更新币种，binance", err)
		return false
	}

	for _, v := range binanceSymbolInfo {
		symbolsMap.Set(v.Symbol, &entity.LhCoinSymbol{
			Id:                0,
			Coin:              "",
			Symbol:            v.Symbol,
			StartTime:         0,
			EndTime:           0,
			PricePrecision:    v.PricePrecision,
			QuantityPrecision: v.QuantityPrecision,
			IsOpen:            0,
		})
	}

	//var (
	//	resGate []gateapi.Contract
	//)
	//
	//resGate, err = getGateContract()
	//if nil != err {
	//	log.Println("更新币种， gate", err)
	//	return false
	//}
	//
	//for _, v := range resGate {
	//	var (
	//		tmp  float64
	//		tmp2 int
	//	)
	//	tmp, err = strconv.ParseFloat(v.QuantoMultiplier, 64)
	//	if nil != err {
	//		continue
	//	}
	//
	//	tmp2 = getDecimalPlaces(v.OrderPriceRound)
	//
	//	base := strings.TrimSuffix(v.Name, "_USDT")
	//	symbolsMapGate.Set(base+"USDT", &SymbolGate{
	//		Symbol:           v.Name,
	//		QuantoMultiplier: tmp,
	//		OrderPriceRound:  tmp2,
	//	})
	//}

	return true
}

func getDecimalPlaces(orderPriceRound string) int {
	parts := strings.Split(orderPriceRound, ".")
	if len(parts) == 2 {
		// 去除末尾多余的 0
		decimals := strings.TrimRight(parts[1], "0")
		return len(decimals)
	}
	return 0
}

// PullAndSetBaseMoneyNewGuiTuAndUser 拉取binance保证金数据
func (s *sBinanceTraderHistory) PullAndSetBaseMoneyNewGuiTuAndUser(ctx context.Context) {
	var (
		err error
		one string
		two string
	)

	one, two, err = requestBinanceTraderDetail(globalTraderNum)
	if nil != err {
		fmt.Println("龟兔，拉取保证金失败：", err, globalTraderNum)
	}
	if 0 < len(one) {
		var tmp float64
		tmp, err = strconv.ParseFloat(one, 64)
		if nil != err {
			fmt.Println("龟兔，拉取保证金，转化失败：", err, globalTraderNum)
		}

		if !IsEqual(tmp, baseMoneyGuiTu.Val()) {
			fmt.Println("龟兔，变更保证金")
			baseMoneyGuiTu.Set(tmp)
		}
	}

	if 0 < len(two) {
		var tmp float64
		tmp, err = strconv.ParseFloat(two, 64)
		if nil != err {
			fmt.Println("龟兔，拉取保证金，规模，转化失败：", err, globalTraderNum)
		}

		if !IsEqual(tmp, baseMoneyTotalGuiTu.Val()) && !IsEqual(tmp, baseMoneyGuiTu.Val()) {
			//fmt.Println("龟兔，变更，规模，保证金")
			if tmp > baseMoneyGuiTu.Val() {
				baseMoneyTotalGuiTu.Set(tmp - baseMoneyGuiTu.Val())
			} else if tmp == baseMoneyGuiTu.Val() {
				baseMoneyTotalGuiTu.Set(tmp)
			} else {
				fmt.Println("龟兔，变更，规模，保证金，小于", tmp, baseMoneyGuiTu.Val())
			}
		}
	}

	var (
		users []*entity.NewUser
	)
	err = g.Model("new_user").Ctx(ctx).Scan(&users)
	if nil != err {
		fmt.Println("龟兔，新增用户，数据库查询错误：", err)
		return
	}

	tmpUserMap := make(map[uint]*entity.NewUser, 0)
	for _, vUsers := range users {
		tmpUserMap[vUsers.Id] = vUsers
	}

	globalUsers.Iterator(func(k interface{}, v interface{}) bool {
		vGlobalUsers := v.(*entity.NewUser)

		if _, ok := tmpUserMap[vGlobalUsers.Id]; !ok {
			fmt.Println("龟兔，变更保证金，用户数据错误，数据库不存在：", vGlobalUsers)
			return true
		}

		tmp := tmpUserMap[vGlobalUsers.Id].Num
		if !baseMoneyUserAllMap.Contains(int(vGlobalUsers.Id)) {
			fmt.Println("初始化成功保证金", vGlobalUsers, tmp)
			baseMoneyUserAllMap.Set(int(vGlobalUsers.Id), tmp)
		} else {
			if !IsEqual(tmp, baseMoneyUserAllMap.Get(int(vGlobalUsers.Id)).(float64)) {
				fmt.Println("变更成功", int(vGlobalUsers.Id), baseMoneyUserAllMap.Get(int(vGlobalUsers.Id)).(float64), tmp)
				baseMoneyUserAllMap.Set(int(vGlobalUsers.Id), tmp)
			}
		}

		return true
	})
}

// InsertGlobalUsersNew  新增用户
func (s *sBinanceTraderHistory) InsertGlobalUsersNew(ctx context.Context) {
	var (
		err   error
		users []*entity.NewUser
	)
	err = g.Model("new_user").Ctx(ctx).
		Where("api_status=?", 1).
		Scan(&users)
	if nil != err {
		fmt.Println("龟兔，新增用户，数据库查询错误：", err)
		return
	}

	tmpUserMap := make(map[uint]*entity.NewUser, 0)
	for _, vUsers := range users {
		tmpUserMap[vUsers.Id] = vUsers
	}

	// 第一遍比较，新增
	for k, vTmpUserMap := range tmpUserMap {
		if globalUsers.Contains(k) {
			vGlobalUsers := globalUsers.Get(k).(*entity.NewUser)
			// 数据变更
			if vTmpUserMap.First != vGlobalUsers.First || // 平仓总次数
				vTmpUserMap.Dai != vGlobalUsers.Dai || // 平仓间隔时长毫秒
				//vTmpUserMap.Second != vGlobalUsers.Second || // 目标开仓，使用的保证金占比限制
				vTmpUserMap.Address != vGlobalUsers.Address ||
				vTmpUserMap.ApiSecret != vGlobalUsers.ApiSecret ||
				vTmpUserMap.BindTraderStatusTfi != vGlobalUsers.BindTraderStatusTfi || // 每多少u
				vTmpUserMap.BindTraderStatus != vGlobalUsers.BindTraderStatus { // 开多少u
				log.Println("用户更新，信息:", vGlobalUsers, vTmpUserMap)
				globalUsers.Set(vTmpUserMap.Id, vTmpUserMap)
			}
		} else {
			tmp := vTmpUserMap.Num
			if !baseMoneyUserAllMap.Contains(int(vTmpUserMap.Id)) {
				fmt.Println("新增用户，初始化成功保证金", vTmpUserMap, tmp)
				baseMoneyUserAllMap.Set(int(vTmpUserMap.Id), tmp)
			} else {
				if !IsEqual(tmp, baseMoneyUserAllMap.Get(int(vTmpUserMap.Id)).(float64)) {
					fmt.Println("新增用户，变更成功", int(vTmpUserMap.Id), tmp, baseMoneyUserAllMap.Get(int(vTmpUserMap.Id)).(float64))
					baseMoneyUserAllMap.Set(int(vTmpUserMap.Id), tmp)
				}
			}

			fmt.Println("龟兔，新增用户:", k, vTmpUserMap)
			globalUsers.Set(k, vTmpUserMap)
		}
	}

	// 第二遍比较，删除
	tmpIds := make([]uint, 0)
	globalUsers.Iterator(func(k interface{}, v interface{}) bool {
		if _, ok := tmpUserMap[k.(uint)]; !ok {
			tmpIds = append(tmpIds, k.(uint))
		}
		return true
	})

	// 删除的人
	for _, vTmpIds := range tmpIds {
		globalUsers.Remove(vTmpIds)
		log.Println("删除用户", vTmpIds)
	}
}

func ceilToNDecimal(x float64, n int) float64 {
	pow := math.Pow(10, float64(n))
	return math.Ceil(x*pow) / pow
}

// SetCookie set cookie
func (s *sBinanceTraderHistory) SetCookie(ctx context.Context, cookie, token string) int64 {
	var (
		err error
	)

	_, err = g.Model("zy_trader_cookie").Ctx(ctx).
		Data(g.Map{"cookie": cookie, "token": token, "is_open": 1}).
		Where("id=?", 1).Update()
	if nil != err {
		log.Println("更新cookie：", err)
		return 0
	}

	return 1
}

// SetExMap set ExMap
func (s *sBinanceTraderHistory) SetExMap(name, res string) int64 {
	if "ok" == res {
		exMap.Set(name, true)
	}

	if "close" == res {
		exMap.Remove(name)
	}

	return 1
}

// GetUsers get users
func (s *sBinanceTraderHistory) GetUsers() []*entity.NewUser {
	res := make([]*entity.NewUser, 0)

	globalUsers.Iterator(func(k interface{}, v interface{}) bool {
		vGlobalUsers := v.(*entity.NewUser)

		res = append(res, vGlobalUsers)
		return true
	})

	return res
}

// CreateUser set user num
func (s *sBinanceTraderHistory) CreateUser(ctx context.Context, address, apiKey, apiSecret string, dai uint64, num, first float64, three, four int64) error {
	var (
		err error
	)

	_, err = g.Model("new_user").Ctx(ctx).Insert(&do.NewUser{
		Address:             address,
		ApiStatus:           1,
		ApiKey:              apiKey,
		ApiSecret:           apiSecret,
		OpenStatus:          2,
		CreatedAt:           gtime.Now(),
		UpdatedAt:           gtime.Now(),
		NeedInit:            1,
		Num:                 num,
		Plat:                "binance",
		Dai:                 dai,
		First:               first,
		Second:              1,
		OrderType:           1,
		Ip:                  1,
		BindTraderStatusTfi: three, // 每多少u
		BindTraderStatus:    four,  // 开多少u
	})

	if nil != err {
		log.Println("新增用户失败：", err)
		return err
	}
	return nil
}

// SetUser set user
func (s *sBinanceTraderHistory) SetUser(ctx context.Context, address, apiKey, apiSecret string, apiStatus, dai uint64, num, first float64, three, four int64) error {
	var (
		err error
	)
	_, err = g.Model("new_user").Ctx(ctx).
		Data(g.Map{
			"num":                    num,
			"api_status":             apiStatus,
			"api_secret":             apiSecret,
			"address":                address,
			"dai":                    dai,
			"first":                  first,
			"bind_trader_status_tfi": three,
			"bind_trader_status":     four,
		}).
		Where("api_key=?", apiKey).Update()
	if nil != err {
		log.Println("更新用户：", err)
		return err
	}

	return nil
}

// SetRunning set running
func (s *sBinanceTraderHistory) SetRunning(res string) int64 {
	if "stop" == res {
		running = false
	}

	if "running" == res {
		running = true
	}

	return 1
}

// SetGlobalTraderNum set globalTraderNum
func (s *sBinanceTraderHistory) SetGlobalTraderNum(res uint64) int64 {
	globalTraderNum = res
	return 1
}

// GetGlobalTraderNum get globalTraderNum
func (s *sBinanceTraderHistory) GetGlobalTraderNum() uint64 {
	return globalTraderNum
}

// GetCookie get cookie
func (s *sBinanceTraderHistory) GetCookie(ctx context.Context) (string, string, int) {
	var (
		err            error
		zyTraderCookie []*entity.ZyTraderCookie
		cookie         = "no"
		token          = "no"
	)

	// 数据库必须信息
	err = g.Model("zy_trader_cookie").Ctx(ctx).Where("trader_id=?", 1).
		OrderDesc("update_time").Limit(1).Scan(&zyTraderCookie)
	if nil != err {
		return cookie, token, 0
	}

	if 0 >= len(zyTraderCookie) {
		return cookie, token, 0
	}

	return zyTraderCookie[0].Cookie, zyTraderCookie[0].Token, zyTraderCookie[0].IsOpen
}

// GetExMap get exMap
func (s *sBinanceTraderHistory) GetExMap() map[string]bool {
	res := make(map[string]bool, 0)
	exMap.Iterator(func(k string, v interface{}) bool {
		res[k] = v.(bool)
		return true
	})

	return res
}

// PullAndOrderNewGuiTuPlay 拉取binance数据，新玩法滑点模式，仓位，根据cookie 龟兔赛跑
func (s *sBinanceTraderHistory) PullAndOrderNewGuiTuPlay(ctx context.Context) {
	var (
		err            error
		traderNum      = globalTraderNum // 龟兔
		zyTraderCookie []*entity.ZyTraderCookie
		cookie         = "no"
		token          = "no"
	)

	exMap.Set("ETHUSDT", true)
	exMap.Set("BTCUSDT", true)
	exMap.Set("1000PEPEUSDT", true)
	exMap.Set("SOLUSDT", true)
	exMap.Set("FILUSDT", true)
	exMap.Set("DOGEUSDT", true)
	exMap.Set("1000SHIBIUSDT", true)
	exMap.Set("BNBUSDT", true)
	exMap.Set("LTCUSDT", true)
	exMap.Set("XRPUSDT", true)

	log.Println("当前带单员id：", traderNum)

	// 执行
	for {
		if !running {
			log.Println("停止程序")

			// 清空运行必须数据
			binancePositionMap = make(map[string]*entity.TraderPosition, 0)
			locKOrder.Clear()
			locKOrderTime.Clear()
			break
		}

		//time.Sleep(5 * time.Second)
		time.Sleep(28 * time.Millisecond)
		start := time.Now()

		var (
			reqResData                []*binancePositionDataList
			binancePositionMapCompare map[string]*entity.TraderPosition
		)
		// 重新初始化数据
		if 0 < len(binancePositionMap) {
			binancePositionMapCompare = make(map[string]*entity.TraderPosition, 0)
			for k, vBinancePositionMap := range binancePositionMap {
				binancePositionMapCompare[k] = vBinancePositionMap
			}
		}

		if "no" == cookie || "no" == token {
			// 数据库必须信息
			err = g.Model("zy_trader_cookie").Ctx(ctx).Where("trader_id=? and is_open=?", 1, 1).
				OrderDesc("update_time").Limit(1).Scan(&zyTraderCookie)
			if nil != err {
				//fmt.Println("龟兔，cookie，数据库查询错误：", err)
				time.Sleep(time.Second * 3)
				continue
			}

			if 0 >= len(zyTraderCookie) || 0 >= len(zyTraderCookie[0].Cookie) || 0 >= len(zyTraderCookie[0].Token) {
				//fmt.Println("龟兔，cookie，无可用：", err)
				time.Sleep(time.Second * 3)
				continue
			}

			// 更新
			cookie = zyTraderCookie[0].Cookie
			token = zyTraderCookie[0].Token
		}

		// 执行
		var (
			retry           = false
			retryTimes      = 0
			retryTimesLimit = 5 // 重试次数
			cookieErr       = false
		)

		for retryTimes < retryTimesLimit { // 最大重试
			// 龟兔的数据
			reqResData, retry, err = s.requestBinancePositionHistoryNew(traderNum, cookie, token)

			// 需要重试
			if retry {
				retryTimes++
				time.Sleep(time.Second * 5)
				log.Println("重试：", retry)
				continue
			}

			// cookie不好使
			if 0 >= len(reqResData) {
				retryTimes++
				cookieErr = true
				continue
			} else {
				cookieErr = false
				break
			}
		}

		// 记录时间
		timePull := time.Since(start)

		// cookie 错误
		if cookieErr {
			cookie = "no"
			token = "no"

			log.Println("cookie错误，信息", traderNum, reqResData)
			err = g.DB().Transaction(context.TODO(), func(ctx context.Context, tx gdb.TX) error {
				// 这里理论上，在极短的时间内完成设置0，覆盖新换上的cookie时设置的1，但是人为操作应该没有那么快不再考虑细节
				zyTraderCookie[0].IsOpen = 0
				_, err = tx.Ctx(ctx).Update("zy_trader_cookie", zyTraderCookie[0], "id", zyTraderCookie[0].Id)
				if nil != err {
					log.Println("cookie错误，信息", traderNum, reqResData)
					return err
				}

				return nil
			})
			if nil != err {
				log.Println("cookie错误，更新数据库错误，信息", traderNum, err)
			}

			continue
		}

		// 用于数据库更新
		insertData := make([]*do.TraderPosition, 0)
		updateData := make([]*do.TraderPosition, 0)
		// 用于下单
		orderInsertData := make([]*do.TraderPosition, 0)
		orderUpdateData := make([]*do.TraderPosition, 0)
		for _, vReqResData := range reqResData {
			// 新增
			var (
				currentAmount    float64
				currentAmountAbs float64
				markPrice        float64
			)
			currentAmount, err = strconv.ParseFloat(vReqResData.PositionAmount, 64)
			if nil != err {
				log.Println("解析金额出错，信息", vReqResData, currentAmount, traderNum)
			}
			currentAmountAbs = math.Abs(currentAmount) // 绝对值

			markPrice, err = strconv.ParseFloat(vReqResData.MarkPrice, 64)
			if nil != err {
				log.Println("解析价格出错，信息", vReqResData, markPrice, traderNum)
			}

			if _, ok := binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide]; !ok {
				if "BOTH" != vReqResData.PositionSide { // 单项持仓
					// 加入数据库
					insertData = append(insertData, &do.TraderPosition{
						Symbol:         vReqResData.Symbol,
						PositionSide:   vReqResData.PositionSide,
						PositionAmount: currentAmountAbs,
						MarkPrice:      markPrice,
					})

					// 下单
					if IsEqual(currentAmountAbs, 0) {
						continue
					}

					orderInsertData = append(orderInsertData, &do.TraderPosition{
						Symbol:         vReqResData.Symbol,
						PositionSide:   vReqResData.PositionSide,
						PositionAmount: currentAmountAbs,
						MarkPrice:      markPrice,
					})
				} else {

					// 加入数据库
					insertData = append(insertData, &do.TraderPosition{
						Symbol:         vReqResData.Symbol,
						PositionSide:   vReqResData.PositionSide,
						PositionAmount: currentAmount, // 正负数保持
						MarkPrice:      markPrice,
					})

					// 模拟为多空仓，下单，todo 组合式的判断应该时牢靠的
					var tmpPositionSide string
					if IsEqual(currentAmount, 0) {
						continue
					} else if math.Signbit(currentAmount) {
						// 模拟空
						tmpPositionSide = "SHORT"
						orderInsertData = append(orderInsertData, &do.TraderPosition{
							Symbol:         vReqResData.Symbol,
							PositionSide:   tmpPositionSide,
							PositionAmount: currentAmountAbs, // 变成绝对值
							MarkPrice:      markPrice,
						})
					} else {
						// 模拟多
						tmpPositionSide = "LONG"
						orderInsertData = append(orderInsertData, &do.TraderPosition{
							Symbol:         vReqResData.Symbol,
							PositionSide:   tmpPositionSide,
							PositionAmount: currentAmountAbs, // 变成绝对值
							MarkPrice:      markPrice,
						})
					}
				}
			} else {
				// 数量无变化
				if "BOTH" != vReqResData.PositionSide {
					if IsEqual(currentAmountAbs, binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].PositionAmount) {
						continue
					}

					updateData = append(updateData, &do.TraderPosition{
						Id:             binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].Id,
						Symbol:         vReqResData.Symbol,
						PositionSide:   vReqResData.PositionSide,
						PositionAmount: currentAmountAbs,
						MarkPrice:      markPrice,
					})

					orderUpdateData = append(orderUpdateData, &do.TraderPosition{
						Id:             binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].Id,
						Symbol:         vReqResData.Symbol,
						PositionSide:   vReqResData.PositionSide,
						PositionAmount: currentAmountAbs,
						MarkPrice:      markPrice,
					})
				} else {

					if IsEqual(currentAmount, binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].PositionAmount) {
						continue
					}

					updateData = append(updateData, &do.TraderPosition{
						Id:             binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].Id,
						Symbol:         vReqResData.Symbol,
						PositionSide:   vReqResData.PositionSide,
						PositionAmount: currentAmount, // 正负数保持
						MarkPrice:      markPrice,
					})

					// 第一步：构造虚拟的上一次仓位，空或多或无
					// 这里修改一下历史仓位的信息，方便程序在后续的流程中使用，模拟both的positionAmount为正数时，修改仓位对应的多仓方向的数据，为负数时修改空仓位的数据，0时不处理
					if _, ok = binancePositionMap[vReqResData.Symbol+"SHORT"]; !ok {
						log.Println("缺少仓位SHORT，信息", binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide])
						continue
					}
					if _, ok = binancePositionMap[vReqResData.Symbol+"LONG"]; !ok {
						log.Println("缺少仓位LONG，信息", binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide])
						continue
					}

					var lastPositionSide string // 上次仓位
					binancePositionMapCompare[vReqResData.Symbol+"SHORT"] = &entity.TraderPosition{
						Id:             binancePositionMapCompare[vReqResData.Symbol+"SHORT"].Id,
						Symbol:         binancePositionMapCompare[vReqResData.Symbol+"SHORT"].Symbol,
						PositionSide:   binancePositionMapCompare[vReqResData.Symbol+"SHORT"].PositionSide,
						PositionAmount: 0,
						CreatedAt:      binancePositionMapCompare[vReqResData.Symbol+"SHORT"].CreatedAt,
						UpdatedAt:      binancePositionMapCompare[vReqResData.Symbol+"SHORT"].UpdatedAt,
						MarkPrice:      markPrice,
					}
					binancePositionMapCompare[vReqResData.Symbol+"LONG"] = &entity.TraderPosition{
						Id:             binancePositionMapCompare[vReqResData.Symbol+"LONG"].Id,
						Symbol:         binancePositionMapCompare[vReqResData.Symbol+"LONG"].Symbol,
						PositionSide:   binancePositionMapCompare[vReqResData.Symbol+"LONG"].PositionSide,
						PositionAmount: 0,
						CreatedAt:      binancePositionMapCompare[vReqResData.Symbol+"LONG"].CreatedAt,
						UpdatedAt:      binancePositionMapCompare[vReqResData.Symbol+"LONG"].UpdatedAt,
						MarkPrice:      markPrice,
					}

					if IsEqual(binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].PositionAmount, 0) { // both仓为0
						// 认为两仓都无

					} else if math.Signbit(binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].PositionAmount) {
						lastPositionSide = "SHORT"
						binancePositionMapCompare[vReqResData.Symbol+"SHORT"].PositionAmount = math.Abs(binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].PositionAmount)
					} else {
						lastPositionSide = "LONG"
						binancePositionMapCompare[vReqResData.Symbol+"LONG"].PositionAmount = math.Abs(binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].PositionAmount)
					}

					// 本次仓位
					var tmpPositionSide string
					if IsEqual(currentAmount, 0) { // 本次仓位是0
						if 0 >= len(lastPositionSide) {
							// 本次和上一次仓位都是0，应该不会走到这里
							log.Println("仓位异常逻辑，信息", binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide])
							continue
						}

						// 仍为是一次完全平仓，仓位和上一次保持一致
						tmpPositionSide = lastPositionSide
					} else if math.Signbit(currentAmount) { // 判断有无符号
						// 第二步：本次仓位

						// 上次和本次相反需要平上次
						if "LONG" == lastPositionSide {
							//orderUpdateData = append(orderUpdateData, &do.TraderPosition{
							//	Id:             binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].Id,
							//	Symbol:         vReqResData.Symbol,
							//	PositionSide:   lastPositionSide,
							//	PositionAmount: float64(0),
							//	MarkPrice:      markPrice,
							//})
						}

						tmpPositionSide = "SHORT"
					} else {
						// 第二步：本次仓位

						// 上次和本次相反需要平上次
						if "SHORT" == lastPositionSide {
							//orderUpdateData = append(orderUpdateData, &do.TraderPosition{
							//	Id:             binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].Id,
							//	Symbol:         vReqResData.Symbol,
							//	PositionSide:   lastPositionSide,
							//	PositionAmount: float64(0),
							//	MarkPrice:      markPrice,
							//})
						}

						tmpPositionSide = "LONG"
					}

					orderUpdateData = append(orderUpdateData, &do.TraderPosition{
						Id:             binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].Id,
						Symbol:         vReqResData.Symbol,
						PositionSide:   tmpPositionSide,
						PositionAmount: currentAmountAbs,
						MarkPrice:      markPrice,
					})
				}
			}
		}

		if 0 >= len(insertData) && 0 >= len(updateData) {
			continue
		}

		// 新增数据
		tmpIdCurrent := len(binancePositionMap) + 1
		for _, vIBinancePosition := range insertData {
			binancePositionMap[vIBinancePosition.Symbol.(string)+vIBinancePosition.PositionSide.(string)] = &entity.TraderPosition{
				Id:             uint(tmpIdCurrent),
				Symbol:         vIBinancePosition.Symbol.(string),
				PositionSide:   vIBinancePosition.PositionSide.(string),
				PositionAmount: vIBinancePosition.PositionAmount.(float64),
				MarkPrice:      vIBinancePosition.MarkPrice.(float64),
			}
		}

		// 更新仓位数据
		for _, vUBinancePosition := range updateData {
			binancePositionMap[vUBinancePosition.Symbol.(string)+vUBinancePosition.PositionSide.(string)] = &entity.TraderPosition{
				Id:             vUBinancePosition.Id.(uint),
				Symbol:         vUBinancePosition.Symbol.(string),
				PositionSide:   vUBinancePosition.PositionSide.(string),
				PositionAmount: vUBinancePosition.PositionAmount.(float64),
				MarkPrice:      vUBinancePosition.MarkPrice.(float64),
			}
		}

		// 推送订单，数据库已初始化仓位，新仓库
		if 0 >= len(binancePositionMapCompare) {
			log.Println("初始化仓位成功")
			continue
		}

		log.Printf("程序拉取部分，开始 %v, 拉取时长: %v, 统计更新时长: %v\n", start, timePull, time.Since(start))

		//wg := sync.WaitGroup{}
		// 遍历跟单者
		tmpTraderBaseMoney := baseMoneyGuiTu.Val()
		if 0 >= tmpTraderBaseMoney {
			log.Printf("带单员保证金为0")
			continue
		}

		tmpTraderBaseMoneyTotal := baseMoneyTotalGuiTu.Val()
		if 0 >= tmpTraderBaseMoneyTotal {
			log.Printf("带单员带单规模保证金为0")
			continue
		}

		globalUsers.Iterator(func(k interface{}, v interface{}) bool {
			tmpUser := v.(*entity.NewUser)

			var tmpUserBindTradersAmount float64
			if !baseMoneyUserAllMap.Contains(int(tmpUser.Id)) {
				log.Println("保证金不存在：", tmpUser)
				return true
			}
			tmpUserBindTradersAmount = baseMoneyUserAllMap.Get(int(tmpUser.Id)).(float64)
			if lessThanOrEqualZero(tmpUserBindTradersAmount, 0, 1e-7) {
				log.Println("保证金不足为0：", tmpUserBindTradersAmount, tmpUser)
				return true
			}

			if 0 >= len(tmpUser.ApiSecret) || 0 >= len(tmpUser.ApiKey) {
				log.Println("用户的信息无效了，信息", traderNum, tmpUser)
				return true
			}

			// 新增仓位
			for _, vInsertData := range orderInsertData {
				tmpInsertData := vInsertData

				if exMap.Contains(tmpInsertData.Symbol.(string)) {
					continue
				}

				if lessThanOrEqualZero(tmpInsertData.PositionAmount.(float64), 0, 1e-7) {
					continue
				}

				if lessThanOrEqualZero(tmpInsertData.MarkPrice.(float64), 0, 1e-7) {
					log.Println("价格信息小于0，信息", tmpInsertData)
					continue
				}

				tmpRate := tmpInsertData.PositionAmount.(float64) * tmpInsertData.MarkPrice.(float64) / tmpTraderBaseMoney
				//if tmpRate < tmpUser.Second {
				//	log.Println("小于操作规定比例，信息", tmpInsertData, tmpTraderBaseMoney, tmpUser.Second)
				//	continue
				//}

				// 所有跟单人的总开仓u
				tmpTotalPositionAmountU := tmpRate * tmpTraderBaseMoneyTotal
				if 1 >= tmpUser.BindTraderStatusTfi || 1 >= tmpUser.BindTraderStatus {
					log.Println("用户设置开仓比例，小于等于1，信息", tmpUser, tmpInsertData)
					continue
				}

				// 计算出本次开仓u数，参数1是每多少u，参数2是开多少u
				if float64(tmpUser.BindTraderStatusTfi) > tmpTotalPositionAmountU {
					log.Println("小于操作规定比例，信息", tmpUser.BindTraderStatusTfi, tmpTotalPositionAmountU)
					continue
				}
				cU := tmpTotalPositionAmountU / float64(tmpUser.BindTraderStatusTfi) * float64(tmpUser.BindTraderStatus)

				if !symbolsMap.Contains(tmpInsertData.Symbol.(string)) {
					log.Println("代币信息无效，信息", tmpInsertData, tmpUser)
					continue
				}

				var (
					tmpQty        float64
					quantity      string
					quantityFloat float64
					side          string
					stopSide      string
					positionSide  string
					orderType     = "MARKET"
				)
				if "LONG" == tmpInsertData.PositionSide {
					positionSide = "LONG"
					side = "BUY"
					stopSide = "SELL"

				} else if "SHORT" == tmpInsertData.PositionSide {
					positionSide = "SHORT"
					side = "SELL"
					stopSide = "BUY"

				} else {
					log.Println("无效信息，信息", tmpInsertData)
					continue
				}

				// 本次 保证金*50倍/币价格
				if cU >= tmpUserBindTradersAmount {
					tmpQty = tmpUserBindTradersAmount / tmpInsertData.MarkPrice.(float64) // 本次开单数量
				} else {
					tmpQty = cU / tmpInsertData.MarkPrice.(float64) // 本次开单数量
				}

				// 精度调整
				if 0 >= symbolsMap.Get(tmpInsertData.Symbol.(string)).(*entity.LhCoinSymbol).QuantityPrecision {
					quantity = fmt.Sprintf("%d", int64(tmpQty))
				} else {
					quantity = strconv.FormatFloat(tmpQty, 'f', symbolsMap.Get(tmpInsertData.Symbol.(string)).(*entity.LhCoinSymbol).QuantityPrecision, 64)
				}

				quantityFloat, err = strconv.ParseFloat(quantity, 64)
				if nil != err {
					log.Println(err)
					continue
				}

				if lessThanOrEqualZero(quantityFloat, 0, 1e-7) {
					log.Println("开仓数量太小:", quantity, symbolsMap.Get(tmpInsertData.Symbol.(string)).(*entity.LhCoinSymbol).QuantityPrecision, tmpQty)
					continue
				}

				tmpNow := time.Now().UTC().Unix()
				strUserId := strconv.FormatUint(uint64(tmpUser.Id), 10)
				if locKOrder.Contains(tmpInsertData.Symbol.(string) + "&" + positionSide + "&" + strUserId) {
					lastOrderFloat := locKOrder.Get(tmpInsertData.Symbol.(string) + "&" + positionSide + "&" + strUserId).(float64)
					if IsEqual(lastOrderFloat, quantityFloat) {
						if locKOrderTime.Contains(tmpInsertData.Symbol.(string) + "&" + positionSide + "&" + strUserId) {
							lastOrderT := locKOrderTime.Get(tmpInsertData.Symbol.(string) + "&" + positionSide + "&" + strUserId).(int64)
							if (tmpNow - 3600*24) < lastOrderT {
								fmt.Println("可能抖动", tmpNow, lastOrderT, tmpInsertData.Symbol.(string)+"&"+positionSide+"&"+strUserId, lastOrderFloat, quantityFloat, side)
								continue
							}
						}
					}
				}
				locKOrder.Set(tmpInsertData.Symbol.(string)+"&"+positionSide+"&"+strUserId, quantityFloat)
				locKOrderTime.Set(tmpInsertData.Symbol.(string)+"&"+positionSide+"&"+strUserId, tmpNow)

				//wg.Add(1)
				err = s.pool.Add(ctx, func(ctx context.Context) {
					//defer wg.Done()

					var (
						quantityPreClose      string
						quantityFloatPreClose float64
						allTimes              = uint64(tmpUser.First)
						perTimeMax            = uint64(tmpUser.Dai)
					)

					if 0 >= allTimes {
						quantityPreClose = quantity
						quantityFloatPreClose = quantityFloat
					} else {
						// 精度调整
						if 0 >= symbolsMap.Get(tmpInsertData.Symbol.(string)).(*entity.LhCoinSymbol).QuantityPrecision {
							quantityPreClose = fmt.Sprintf("%d", int64(math.Ceil(quantityFloat/float64(allTimes))))
						} else {
							tmp := ceilToNDecimal(quantityFloat/float64(allTimes), symbolsMap.Get(tmpInsertData.Symbol.(string)).(*entity.LhCoinSymbol).QuantityPrecision)
							quantityPreClose = strconv.FormatFloat(tmp, 'f', symbolsMap.Get(tmpInsertData.Symbol.(string)).(*entity.LhCoinSymbol).QuantityPrecision, 64)
						}

						quantityFloatPreClose, err = strconv.ParseFloat(quantityPreClose, 64)
						if nil != err {
							log.Println(err)
							return
						}

						if lessThanOrEqualZero(quantityFloatPreClose, 0, 1e-7) {
							log.Println("每次，关仓数量太小:", quantityFloatPreClose, symbolsMap.Get(tmpInsertData.Symbol.(string)).(*entity.LhCoinSymbol).QuantityPrecision, quantityFloat)
							return
						}
					}

					var (
						binanceOrderRes *binanceOrder
						orderInfoRes    *orderInfo
						errA            error
					)
					// 请求下单
					binanceOrderRes, orderInfoRes, errA = requestBinanceOrder(tmpInsertData.Symbol.(string), side, orderType, positionSide, quantity, tmpUser.ApiKey, tmpUser.ApiSecret)
					if nil != errA || binanceOrderRes.OrderId <= 0 {
						log.Println("添加下单，信息：", errA, binanceOrderRes, orderInfoRes, tmpInsertData, side, orderType, positionSide, quantity, tmpUser.Id)
						return
					}

					log.Println("新增仓位，完成：", err, quantity, tmpUser.Id)

					// 过了时间立马平掉
					if 0 >= allTimes {
						var (
							binanceOrderRes3 *binanceOrder
							orderInfoRes3    *orderInfo
							errC             error
						)
						// 请求下单
						binanceOrderRes3, orderInfoRes3, errC = requestBinanceOrder(tmpInsertData.Symbol.(string), stopSide, orderType, positionSide, quantity, tmpUser.ApiKey, tmpUser.ApiSecret)
						if nil != errC || binanceOrderRes3.OrderId <= 0 {
							log.Println("关仓，信息：", errC, binanceOrderRes3, orderInfoRes3, tmpInsertData, stopSide, orderType, positionSide, quantity, tmpUser.Id)
							return
						}

						log.Println("新增仓位，平仓：", err, quantity, tmpUser.Id, binanceOrderRes3)
						return
					} else {
						for tmpI := uint64(0); tmpI < allTimes; tmpI++ {
							tmpCloseQty := quantityPreClose
							if tmpI == allTimes-1 {
								tmpCloseQty = quantity
							}

							time.Sleep(time.Duration(perTimeMax) * time.Millisecond)

							//wg.Add(1)
							err = s.pool.Add(ctx, func(ctx context.Context) {
								//defer wg.Done()
								var (
									binanceOrderRes3 *binanceOrder
									orderInfoRes3    *orderInfo
									errC             error
								)
								// 请求下单
								binanceOrderRes3, orderInfoRes3, errC = requestBinanceOrder(tmpInsertData.Symbol.(string), stopSide, orderType, positionSide, tmpCloseQty, tmpUser.ApiKey, tmpUser.ApiSecret)
								if nil != errC || binanceOrderRes3.OrderId <= 0 {
									log.Println("关仓，信息：", errC, binanceOrderRes3, orderInfoRes3, tmpInsertData, stopSide, orderType, positionSide, tmpCloseQty, tmpUser.Id)
									return
								}

								log.Println("新增仓位，平仓：", err, tmpCloseQty, tmpUser.Id, binanceOrderRes3)
								return
							})
							if nil != err {
								log.Println("添加下单任务异常，新增仓位，错误信息：", err, tmpInsertData, tmpUser)
							}
						}
					}
					return
				})
				if nil != err {
					fmt.Println("添加下单任务异常，新增仓位，错误信息：", err, tmpInsertData, tmpUser)
				}

			}

			// 修改仓位
			for _, vUpdateData := range orderUpdateData {
				tmpUpdateData := vUpdateData

				if exMap.Contains(tmpUpdateData.Symbol.(string)) {
					continue
				}

				if _, ok := binancePositionMapCompare[tmpUpdateData.Symbol.(string)+tmpUpdateData.PositionSide.(string)]; !ok {
					log.Println("添加下单任务异常，修改仓位，错误信息：", err, traderNum, tmpUpdateData, tmpUser)
					continue
				}
				lastPositionData := binancePositionMapCompare[tmpUpdateData.Symbol.(string)+tmpUpdateData.PositionSide.(string)]

				if lessThanOrEqualZero(tmpUpdateData.MarkPrice.(float64), 0, 1e-7) {
					log.Println("变更，价格信息小于0，信息", tmpUpdateData)
					continue
				}

				tmpRate := math.Abs(lastPositionData.PositionAmount*lastPositionData.MarkPrice-tmpUpdateData.PositionAmount.(float64)*tmpUpdateData.MarkPrice.(float64)) / tmpTraderBaseMoney
				//if tmpRate < tmpUser.Second {
				//	log.Println("变更，小于操作规定比例，信息", lastPositionData, tmpUpdateData, tmpTraderBaseMoney, tmpUser.Second)
				//	continue
				//}

				// 所有跟单人的总开仓u
				tmpTotalPositionAmountU := tmpRate * tmpTraderBaseMoneyTotal
				if 1 >= tmpUser.BindTraderStatusTfi || 1 >= tmpUser.BindTraderStatus {
					log.Println("用户设置开仓比例，小于等于1，信息", tmpUser, tmpUpdateData)
					continue
				}

				// 计算出本次开仓u数，参数1是每多少u，参数2是开多少u
				if float64(tmpUser.BindTraderStatusTfi) > tmpTotalPositionAmountU {
					log.Println("变更，小于操作规定比例，信息", tmpUser.BindTraderStatusTfi, tmpTotalPositionAmountU)
					continue
				}
				cU := tmpTotalPositionAmountU / float64(tmpUser.BindTraderStatusTfi) * float64(tmpUser.BindTraderStatus)

				if !symbolsMap.Contains(tmpUpdateData.Symbol.(string)) {
					log.Println("代币信息无效，信息", tmpUpdateData, tmpUser)
					continue
				}

				var (
					tmpQty        float64
					quantity      string
					quantityFloat float64
					side          string
					stopSide      string
					positionSide  string
					orderType     = "MARKET"
				)

				if lessThanOrEqualZero(tmpUpdateData.PositionAmount.(float64), 0, 1e-7) {
					log.Println("完全平仓：", tmpUpdateData)
					// 全平仓则，开仓反向
					if "LONG" == tmpUpdateData.PositionSide {
						positionSide = "SHORT"
						side = "SELL"
						stopSide = "BUY"

					} else if "SHORT" == tmpUpdateData.PositionSide {
						positionSide = "LONG"
						side = "BUY"
						stopSide = "SELL"

					} else {
						log.Println("无效信息，信息", tmpUpdateData)
						continue
					}

				} else if lessThanOrEqualZero(lastPositionData.PositionAmount, tmpUpdateData.PositionAmount.(float64), 1e-7) {
					log.Println("追加仓位：", tmpUpdateData, lastPositionData)
					// 本次加仓 代单员币的数量 * (用户保证金/代单员保证金)
					if "LONG" == tmpUpdateData.PositionSide {
						positionSide = "LONG"
						side = "BUY"
						stopSide = "SELL"

					} else if "SHORT" == tmpUpdateData.PositionSide {
						positionSide = "SHORT"
						side = "SELL"
						stopSide = "BUY"

					} else {
						log.Println("无效信息，信息", tmpUpdateData)
						continue
					}

				} else if lessThanOrEqualZero(tmpUpdateData.PositionAmount.(float64), lastPositionData.PositionAmount, 1e-7) {
					log.Println("部分平仓：", tmpUpdateData, lastPositionData)
					// 部分平仓
					if "LONG" == tmpUpdateData.PositionSide {
						positionSide = "SHORT"
						side = "SELL"
						stopSide = "BUY"

					} else if "SHORT" == tmpUpdateData.PositionSide {
						positionSide = "LONG"
						side = "BUY"
						stopSide = "SELL"

					} else {
						log.Println("无效信息，信息", tmpUpdateData)
						continue
					}

					// 上次仓位
					if lessThanOrEqualZero(lastPositionData.PositionAmount, 0, 1e-7) {
						log.Println("部分平仓，上次仓位信息无效，信息", lastPositionData, tmpUpdateData)
						continue
					}

				} else {
					log.Println("分析仓位无效，信息", lastPositionData, tmpUpdateData)
					continue
				}

				// 本次 保证金*50倍/币价格
				if cU >= tmpUserBindTradersAmount {
					tmpQty = tmpUserBindTradersAmount / tmpUpdateData.MarkPrice.(float64) // 本次开单数量
				} else {
					tmpQty = cU / tmpUpdateData.MarkPrice.(float64) // 本次开单数量
				}

				// 精度调整
				if 0 >= symbolsMap.Get(tmpUpdateData.Symbol.(string)).(*entity.LhCoinSymbol).QuantityPrecision {
					quantity = fmt.Sprintf("%d", int64(tmpQty))
				} else {
					quantity = strconv.FormatFloat(tmpQty, 'f', symbolsMap.Get(tmpUpdateData.Symbol.(string)).(*entity.LhCoinSymbol).QuantityPrecision, 64)
				}

				quantityFloat, err = strconv.ParseFloat(quantity, 64)
				if nil != err {
					log.Println(err)
					continue
				}

				if lessThanOrEqualZero(quantityFloat, 0, 1e-7) {
					log.Println("开仓数量太小:", quantity, symbolsMap.Get(tmpUpdateData.Symbol.(string)).(*entity.LhCoinSymbol).QuantityPrecision, tmpQty)
					continue
				}

				tmpNow := time.Now().UTC().Unix()
				strUserId := strconv.FormatUint(uint64(tmpUser.Id), 10)
				if locKOrder.Contains(tmpUpdateData.Symbol.(string) + "&" + positionSide + "&" + strUserId) {
					lastOrderFloat := locKOrder.Get(tmpUpdateData.Symbol.(string) + "&" + positionSide + "&" + strUserId).(float64)
					if IsEqual(lastOrderFloat, quantityFloat) {
						if locKOrderTime.Contains(tmpUpdateData.Symbol.(string) + "&" + positionSide + "&" + strUserId) {
							lastOrderT := locKOrderTime.Get(tmpUpdateData.Symbol.(string) + "&" + positionSide + "&" + strUserId).(int64)
							if (tmpNow - 3600*24) < lastOrderT {
								fmt.Println("可能抖动", tmpUpdateData.Symbol.(string)+"&"+positionSide+"&"+strUserId, lastOrderFloat, quantityFloat, side)
								continue
							}
						}
					}
				}
				locKOrder.Set(tmpUpdateData.Symbol.(string)+"&"+positionSide+"&"+strUserId, quantityFloat)
				locKOrderTime.Set(tmpUpdateData.Symbol.(string)+"&"+positionSide+"&"+strUserId, tmpNow)

				//wg.Add(1)
				err = s.pool.Add(ctx, func(ctx context.Context) {
					//defer wg.Done()

					var (
						quantityPreClose      string
						quantityFloatPreClose float64
						allTimes              = uint64(tmpUser.First)
						perTimeMax            = uint64(tmpUser.Dai)
					)

					if 0 >= allTimes {
						quantityPreClose = quantity
						quantityFloatPreClose = quantityFloat
					} else {
						// 精度调整
						if 0 >= symbolsMap.Get(tmpUpdateData.Symbol.(string)).(*entity.LhCoinSymbol).QuantityPrecision {
							quantityPreClose = fmt.Sprintf("%d", int64(math.Ceil(quantityFloat/float64(allTimes))))
						} else {
							tmp := ceilToNDecimal(quantityFloat/float64(allTimes), symbolsMap.Get(tmpUpdateData.Symbol.(string)).(*entity.LhCoinSymbol).QuantityPrecision)
							quantityPreClose = strconv.FormatFloat(tmp, 'f', symbolsMap.Get(tmpUpdateData.Symbol.(string)).(*entity.LhCoinSymbol).QuantityPrecision, 64)
						}

						quantityFloatPreClose, err = strconv.ParseFloat(quantityPreClose, 64)
						if nil != err {
							log.Println(err)
							return
						}

						if lessThanOrEqualZero(quantityFloatPreClose, 0, 1e-7) {
							log.Println("每次，关仓数量太小:", quantityFloatPreClose, symbolsMap.Get(tmpUpdateData.Symbol.(string)).(*entity.LhCoinSymbol).QuantityPrecision, quantityFloat)
							return
						}
					}

					var (
						binanceOrderRes *binanceOrder
						orderInfoRes    *orderInfo
						errA            error
					)
					// 请求下单
					binanceOrderRes, orderInfoRes, errA = requestBinanceOrder(tmpUpdateData.Symbol.(string), side, orderType, positionSide, quantity, tmpUser.ApiKey, tmpUser.ApiSecret)
					if nil != errA || binanceOrderRes.OrderId <= 0 {
						log.Println("变更仓位，下单，信息：", errA, binanceOrderRes, orderInfoRes, tmpUpdateData, side, orderType, positionSide, quantity, tmpUser.Id)
						return
					}

					log.Println("变更仓位，完成：", err, quantity, tmpUser.Id)

					// 过了时间立马平掉
					if 0 >= allTimes {
						var (
							binanceOrderRes3 *binanceOrder
							orderInfoRes3    *orderInfo
							errC             error
						)
						// 请求下单
						binanceOrderRes3, orderInfoRes3, errC = requestBinanceOrder(tmpUpdateData.Symbol.(string), stopSide, orderType, positionSide, quantity, tmpUser.ApiKey, tmpUser.ApiSecret)
						if nil != errC || binanceOrderRes3.OrderId <= 0 {
							log.Println("关仓，信息：", errC, binanceOrderRes3, orderInfoRes3, tmpUpdateData, stopSide, orderType, positionSide, quantity, tmpUser.Id)
							return
						}

						log.Println("更新仓位，平仓：", err, quantity, tmpUser.Id, binanceOrderRes3)
						return
					} else {
						for tmpI := uint64(0); tmpI < allTimes; tmpI++ {
							tmpCloseQty := quantityPreClose
							if tmpI == allTimes-1 {
								tmpCloseQty = quantity
							}

							time.Sleep(time.Duration(perTimeMax) * time.Millisecond)

							//wg.Add(1)
							err = s.pool.Add(ctx, func(ctx context.Context) {
								//defer wg.Done()
								var (
									binanceOrderRes3 *binanceOrder
									orderInfoRes3    *orderInfo
									errC             error
								)
								// 请求下单
								binanceOrderRes3, orderInfoRes3, errC = requestBinanceOrder(tmpUpdateData.Symbol.(string), stopSide, orderType, positionSide, tmpCloseQty, tmpUser.ApiKey, tmpUser.ApiSecret)
								if nil != errC || binanceOrderRes3.OrderId <= 0 {
									log.Println("关仓，信息：", errC, binanceOrderRes3, orderInfoRes3, tmpUpdateData, stopSide, orderType, positionSide, tmpCloseQty, tmpUser.Id)
									return
								}

								log.Println("更新仓位，平仓：", err, tmpCloseQty, tmpUser.Id, binanceOrderRes3)
								return
							})
							if nil != err {
								log.Println("更新仓位，下单任务异常，错误信息：", err, tmpUpdateData, tmpUser)
							}
						}
					}
					return
				})
				if nil != err {
					fmt.Println("龟兔，添加下单任务异常，更新仓位，错误信息：", err, tmpUpdateData, tmpUser)
				}
			}

			return true
		})

		// 回收协程
		//wg.Wait()

		log.Printf("程序执行完毕，开始 %v, 拉取时长: %v, 总计时长: %v\n", start, timePull, time.Since(start))
	}
}

type binanceTradeHistoryResp struct {
	Data *binanceTradeHistoryData
}

type binanceTradeHistoryData struct {
	Total uint64
	List  []*binanceTradeHistoryDataList
}

type binanceTradeHistoryDataList struct {
	Time                uint64
	Symbol              string
	Side                string
	Price               float64
	Fee                 float64
	FeeAsset            string
	Quantity            float64
	QuantityAsset       string
	RealizedProfit      float64
	RealizedProfitAsset string
	BaseAsset           string
	Qty                 float64
	PositionSide        string
	ActiveBuy           bool
}

type binancePositionResp struct {
	Data []*binancePositionDataList
}

type binancePositionDataList struct {
	Symbol         string
	PositionSide   string
	PositionAmount string
	MarkPrice      string
}

type binancePositionHistoryResp struct {
	Data *binancePositionHistoryData
}

type binancePositionHistoryData struct {
	Total uint64
	List  []*binancePositionHistoryDataList
}

type binancePositionHistoryDataList struct {
	Time   uint64
	Symbol string
	Side   string
	Opened uint64
	Closed uint64
	Status string
}

type binanceTrade struct {
	TraderNum uint64
	Time      uint64
	Symbol    string
	Type      string
	Position  string
	Side      string
	Price     string
	Qty       string
	QtyFloat  float64
}

type Data struct {
	Symbol     string `json:"symbol"`
	Type       string `json:"type"`
	Price      string `json:"price"`
	Side       string `json:"side"`
	Qty        string `json:"qty"`
	Proportion string `json:"proportion"`
	Position   string `json:"position"`
}

type Order struct {
	Uid       uint64  `json:"uid"`
	BaseMoney string  `json:"base_money"`
	Data      []*Data `json:"data"`
	InitOrder uint64  `json:"init_order"`
	Rate      string  `json:"rate"`
	TraderNum uint64  `json:"trader_num"`
}

type SendBody struct {
	Orders    []*Order `json:"orders"`
	InitOrder uint64   `json:"init_order"`
}

type ListenTraderAndUserOrderRequest struct {
	SendBody SendBody `json:"send_body"`
}

type RequestResp struct {
	Status string
}

// 请求下单接口
func (s *sBinanceTraderHistory) requestSystemOrder(Orders []*Order) (string, error) {
	var (
		resp   *http.Response
		b      []byte
		err    error
		apiUrl = "http://127.0.0.1:8125/api/binanceexchange_user/listen_trader_and_user_order_new"
	)

	// 构造请求数据
	requestBody := ListenTraderAndUserOrderRequest{
		SendBody: SendBody{
			Orders: Orders,
		},
	}

	// 序列化为JSON
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return "", err
	}

	// 创建http.Client并设置超时时间
	client := &http.Client{
		Timeout: 20 * time.Second,
	}

	// 构造http请求
	req, err := http.NewRequest("POST", apiUrl, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	// 发送请求
	resp, err = client.Do(req)
	if err != nil {
		return "", err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(resp.Body)

	b, err = io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return "", err
	}

	var r *RequestResp
	err = json.Unmarshal(b, &r)
	if err != nil {
		fmt.Println(err)
		return "", err
	}

	return r.Status, nil
}

// 请求binance的下单历史接口
func (s *sBinanceTraderHistory) requestProxyBinanceTradeHistory(proxyAddr string, pageNumber int64, pageSize int64, portfolioId uint64) ([]*binanceTradeHistoryDataList, bool, error) {
	var (
		resp   *http.Response
		res    []*binanceTradeHistoryDataList
		b      []byte
		err    error
		apiUrl = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/trade-history"
	)

	proxy, err := url.Parse(proxyAddr)
	if err != nil {
		fmt.Println(err)
		return nil, true, err
	}
	netTransport := &http.Transport{
		Proxy:                 http.ProxyURL(proxy),
		MaxIdleConnsPerHost:   10,
		ResponseHeaderTimeout: time.Second * time.Duration(5),
	}
	httpClient := &http.Client{
		Timeout:   time.Second * 10,
		Transport: netTransport,
	}

	// 构造请求
	contentType := "application/json"
	data := `{"pageNumber":` + strconv.FormatInt(pageNumber, 10) + `,"pageSize":` + strconv.FormatInt(pageSize, 10) + `,portfolioId:` + strconv.FormatUint(portfolioId, 10) + `}`
	resp, err = httpClient.Post(apiUrl, contentType, strings.NewReader(data))
	if err != nil {
		fmt.Println(333, err)
		return nil, true, err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(222, err)
		}
	}(resp.Body)

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(111, err)
		return nil, true, err
	}

	var l *binanceTradeHistoryResp
	err = json.Unmarshal(b, &l)
	if err != nil {
		return nil, true, err
	}

	if nil == l.Data {
		return res, true, nil
	}

	res = make([]*binanceTradeHistoryDataList, 0)
	if nil == l.Data.List {
		return res, false, nil
	}

	res = make([]*binanceTradeHistoryDataList, 0)
	for _, v := range l.Data.List {
		res = append(res, v)
	}

	return res, false, nil
}

// 请求binance的仓位历史接口
func (s *sBinanceTraderHistory) requestProxyBinancePositionHistory(proxyAddr string, pageNumber int64, pageSize int64, portfolioId uint64) ([]*binancePositionHistoryDataList, bool, error) {
	var (
		resp   *http.Response
		res    []*binancePositionHistoryDataList
		b      []byte
		err    error
		apiUrl = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/position-history"
	)

	proxy, err := url.Parse(proxyAddr)
	if err != nil {
		fmt.Println(err)
		return nil, true, err
	}
	netTransport := &http.Transport{
		Proxy:                 http.ProxyURL(proxy),
		MaxIdleConnsPerHost:   10,
		ResponseHeaderTimeout: time.Second * time.Duration(5),
	}
	httpClient := &http.Client{
		Timeout:   time.Second * 10,
		Transport: netTransport,
	}

	// 构造请求
	contentType := "application/json"
	data := `{"sort":"OPENING","pageNumber":` + strconv.FormatInt(pageNumber, 10) + `,"pageSize":` + strconv.FormatInt(pageSize, 10) + `,portfolioId:` + strconv.FormatUint(portfolioId, 10) + `}`
	resp, err = httpClient.Post(apiUrl, contentType, strings.NewReader(data))
	if err != nil {
		fmt.Println(333, err)
		return nil, true, err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(222, err)
		}
	}(resp.Body)

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(111, err)
		return nil, true, err
	}

	var l *binancePositionHistoryResp
	err = json.Unmarshal(b, &l)
	if err != nil {
		return nil, true, err
	}

	if nil == l.Data {
		return res, true, nil
	}

	res = make([]*binancePositionHistoryDataList, 0)
	if nil == l.Data.List {
		return res, false, nil
	}

	res = make([]*binancePositionHistoryDataList, 0)
	for _, v := range l.Data.List {
		res = append(res, v)
	}

	return res, false, nil
}

// 请求binance的持有仓位历史接口，新
func (s *sBinanceTraderHistory) requestProxyBinancePositionHistoryNew(proxyAddr string, portfolioId uint64, cookie string, token string) ([]*binancePositionDataList, bool, error) {
	var (
		resp   *http.Response
		res    []*binancePositionDataList
		b      []byte
		err    error
		apiUrl = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-data/positions?portfolioId=" + strconv.FormatUint(portfolioId, 10)
	)

	proxy, err := url.Parse(proxyAddr)
	if err != nil {
		fmt.Println(err)
		return nil, true, err
	}
	netTransport := &http.Transport{
		Proxy:                 http.ProxyURL(proxy),
		MaxIdleConnsPerHost:   10,
		ResponseHeaderTimeout: time.Second * time.Duration(5),
	}
	httpClient := &http.Client{
		Timeout:   time.Second * 2,
		Transport: netTransport,
	}

	// 构造请求
	req, err := http.NewRequest("GET", apiUrl, nil)
	if err != nil {
		fmt.Println("Error creating request:", err)
		fmt.Println(444444, err)
		return nil, true, err
	}

	// 添加头信息
	req.Header.Set("Clienttype", "web")
	req.Header.Set("Cookie", cookie)
	req.Header.Set("Csrftoken", token)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0")

	// 构造请求
	resp, err = httpClient.Do(req)
	if err != nil {
		fmt.Println("Error making request:", err)
		fmt.Println(444444, err)
		return nil, true, err
	}

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(4444, err)
		return nil, true, err
	}

	var l *binancePositionResp
	err = json.Unmarshal(b, &l)
	if err != nil {
		return nil, true, err
	}

	if nil == l.Data {
		return res, true, nil
	}

	res = make([]*binancePositionDataList, 0)
	for _, v := range l.Data {
		res = append(res, v)
	}

	return res, false, nil
}

// 请求binance的持有仓位历史接口，新
func (s *sBinanceTraderHistory) requestBinancePositionHistoryNew(portfolioId uint64, cookie string, token string) ([]*binancePositionDataList, bool, error) {
	var (
		resp   *http.Response
		res    []*binancePositionDataList
		b      []byte
		err    error
		apiUrl = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-data/positions?portfolioId=" + strconv.FormatUint(portfolioId, 10)
	)

	// 创建不验证 SSL 证书的 HTTP 客户端
	httpClient := &http.Client{
		Timeout: time.Second * 2,
	}

	// 构造请求
	req, err := http.NewRequest("GET", apiUrl, nil)
	if err != nil {
		fmt.Println("Error creating request:", err)
		return nil, true, err
	}

	// 添加头信息
	req.Header.Set("Clienttype", "web")
	req.Header.Set("Cookie", cookie)
	req.Header.Set("Csrftoken", token)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0")

	// 发送请求
	resp, err = httpClient.Do(req)
	if err != nil {
		fmt.Println("Error making request:", err)
		return nil, true, err
	}

	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(44444, err)
		}
	}(resp.Body)

	// 结果
	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(4444, err)
		return nil, true, err
	}

	//fmt.Println(string(b))
	var l *binancePositionResp
	err = json.Unmarshal(b, &l)
	if err != nil {
		return nil, true, err
	}

	if nil == l.Data {
		return res, true, nil
	}

	res = make([]*binancePositionDataList, 0)
	for _, v := range l.Data {
		res = append(res, v)
	}

	return res, false, nil
}

// 暂时弃用
func (s *sBinanceTraderHistory) requestOrder(pageNumber int64, pageSize int64, portfolioId uint64) ([]*binanceTradeHistoryDataList, error) {
	var (
		resp   *http.Response
		res    []*binanceTradeHistoryDataList
		b      []byte
		err    error
		apiUrl = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/trade-history"
	)

	// 构造请求
	contentType := "application/json"
	data := `{"pageNumber":` + strconv.FormatInt(pageNumber, 10) + `,"pageSize":` + strconv.FormatInt(pageSize, 10) + `,portfolioId:` + strconv.FormatUint(portfolioId, 10) + `}`
	resp, err = http.Post(apiUrl, contentType, strings.NewReader(data))
	if err != nil {
		return nil, err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(resp.Body)

	b, err = ioutil.ReadAll(resp.Body)
	//fmt.Println(string(b))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	var l *binanceTradeHistoryResp
	err = json.Unmarshal(b, &l)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	if nil == l.Data {
		return res, nil
	}

	if nil == l.Data.List {
		return res, nil
	}

	res = make([]*binanceTradeHistoryDataList, 0)
	for _, v := range l.Data.List {
		res = append(res, v)
	}

	return res, nil
}

// 暂时弃用
func (s *sBinanceTraderHistory) requestBinanceTradeHistory(pageNumber int64, pageSize int64, portfolioId uint64) ([]*binanceTradeHistoryDataList, error) {
	var (
		resp   *http.Response
		res    []*binanceTradeHistoryDataList
		b      []byte
		err    error
		apiUrl = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/trade-history"
	)

	// 构造请求
	contentType := "application/json"
	data := `{"pageNumber":` + strconv.FormatInt(pageNumber, 10) + `,"pageSize":` + strconv.FormatInt(pageSize, 10) + `,portfolioId:` + strconv.FormatUint(portfolioId, 10) + `}`
	resp, err = http.Post(apiUrl, contentType, strings.NewReader(data))
	if err != nil {
		return nil, err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(resp.Body)

	b, err = ioutil.ReadAll(resp.Body)
	//fmt.Println(string(b))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	var l *binanceTradeHistoryResp
	err = json.Unmarshal(b, &l)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	if nil == l.Data {
		return res, nil
	}

	if nil == l.Data.List {
		return res, nil
	}

	res = make([]*binanceTradeHistoryDataList, 0)
	for _, v := range l.Data.List {
		res = append(res, v)
	}

	return res, nil
}

type binanceOrder struct {
	OrderId       int64
	ExecutedQty   string
	ClientOrderId string
	Symbol        string
	AvgPrice      string
	CumQuote      string
	Side          string
	PositionSide  string
	ClosePosition bool
	Type          string
	Status        string
}

type orderInfo struct {
	Code int64
	Msg  string
}

func requestBinanceOrder(symbol string, side string, orderType string, positionSide string, quantity string, apiKey string, secretKey string) (*binanceOrder, *orderInfo, error) {
	var (
		client       *http.Client
		req          *http.Request
		resp         *http.Response
		res          *binanceOrder
		resOrderInfo *orderInfo
		data         string
		b            []byte
		err          error
		apiUrl       = "https://fapi.binance.com/fapi/v1/order"
	)

	//fmt.Println(symbol, side, orderType, positionSide, quantity, apiKey, secretKey)
	// 时间
	now := strconv.FormatInt(time.Now().UTC().UnixMilli(), 10)
	// 拼请求数据
	data = "symbol=" + symbol + "&side=" + side + "&type=" + orderType + "&positionSide=" + positionSide + "&newOrderRespType=" + "RESULT" + "&quantity=" + quantity + "&timestamp=" + now

	// 加密
	h := hmac.New(sha256.New, []byte(secretKey))
	h.Write([]byte(data))
	signature := hex.EncodeToString(h.Sum(nil))
	// 构造请求

	req, err = http.NewRequest("POST", apiUrl, strings.NewReader(data+"&signature="+signature))
	if err != nil {
		return nil, nil, err
	}
	// 添加头信息
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-MBX-APIKEY", apiKey)

	// 请求执行
	client = &http.Client{Timeout: 3 * time.Second}
	resp, err = client.Do(req)
	if err != nil {
		return nil, nil, err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(resp.Body)

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(string(b), err)
		return nil, nil, err
	}

	var o binanceOrder
	err = json.Unmarshal(b, &o)
	if err != nil {
		fmt.Println(string(b), err)
		return nil, nil, err
	}

	res = &binanceOrder{
		OrderId:       o.OrderId,
		ExecutedQty:   o.ExecutedQty,
		ClientOrderId: o.ClientOrderId,
		Symbol:        o.Symbol,
		AvgPrice:      o.AvgPrice,
		CumQuote:      o.CumQuote,
		Side:          o.Side,
		PositionSide:  o.PositionSide,
		ClosePosition: o.ClosePosition,
		Type:          o.Type,
	}

	if 0 >= res.OrderId {
		//fmt.Println(string(b))
		err = json.Unmarshal(b, &resOrderInfo)
		if err != nil {
			fmt.Println(string(b), err)
			return nil, nil, err
		}
	}

	return res, resOrderInfo, nil
}

func requestBinanceOrderStop(symbol string, side string, positionSide string, quantity string, stopPrice string, price string, apiKey string, secretKey string) (*binanceOrder, *orderInfo, error) {
	//fmt.Println(symbol, side, positionSide, quantity, stopPrice, price, apiKey, secretKey)
	var (
		client       *http.Client
		req          *http.Request
		resp         *http.Response
		res          *binanceOrder
		resOrderInfo *orderInfo
		data         string
		b            []byte
		err          error
		apiUrl       = "https://fapi.binance.com/fapi/v1/order"
	)

	// 时间
	now := strconv.FormatInt(time.Now().UTC().UnixMilli(), 10)
	// 拼请求数据
	data = "symbol=" + symbol + "&side=" + side + "&type=STOP_MARKET&stopPrice=" + stopPrice + "&positionSide=" + positionSide + "&newOrderRespType=" + "RESULT" + "&quantity=" + quantity + "&timestamp=" + now

	// 加密
	h := hmac.New(sha256.New, []byte(secretKey))
	h.Write([]byte(data))
	signature := hex.EncodeToString(h.Sum(nil))
	// 构造请求

	req, err = http.NewRequest("POST", apiUrl, strings.NewReader(data+"&signature="+signature))
	if err != nil {
		return nil, nil, err
	}
	// 添加头信息
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-MBX-APIKEY", apiKey)

	// 请求执行
	client = &http.Client{Timeout: 3 * time.Second}
	resp, err = client.Do(req)
	if err != nil {
		return nil, nil, err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(resp.Body)

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(string(b), err)
		return nil, nil, err
	}

	var o binanceOrder
	err = json.Unmarshal(b, &o)
	if err != nil {
		fmt.Println(string(b), err)
		return nil, nil, err
	}

	res = &binanceOrder{
		OrderId:       o.OrderId,
		ExecutedQty:   o.ExecutedQty,
		ClientOrderId: o.ClientOrderId,
		Symbol:        o.Symbol,
		AvgPrice:      o.AvgPrice,
		CumQuote:      o.CumQuote,
		Side:          o.Side,
		PositionSide:  o.PositionSide,
		ClosePosition: o.ClosePosition,
		Type:          o.Type,
	}

	if 0 >= res.OrderId {
		//fmt.Println(string(b))
		err = json.Unmarshal(b, &resOrderInfo)
		if err != nil {
			fmt.Println(string(b), err)
			return nil, nil, err
		}
	}

	return res, resOrderInfo, nil
}

func requestBinanceOrderStopTakeProfit(symbol string, side string, positionSide string, quantity string, stopPrice string, price string, apiKey string, secretKey string) (*binanceOrder, *orderInfo, error) {
	//fmt.Println(symbol, side, positionSide, quantity, stopPrice, price, apiKey, secretKey)
	var (
		client       *http.Client
		req          *http.Request
		resp         *http.Response
		res          *binanceOrder
		resOrderInfo *orderInfo
		data         string
		b            []byte
		err          error
		apiUrl       = "https://fapi.binance.com/fapi/v1/order"
	)

	// 时间
	now := strconv.FormatInt(time.Now().UTC().UnixMilli(), 10)
	// 拼请求数据
	data = "symbol=" + symbol + "&side=" + side + "&type=TAKE_PROFIT&stopPrice=" + stopPrice + "&price=" + price + "&positionSide=" + positionSide + "&newOrderRespType=" + "RESULT" + "&quantity=" + quantity + "&timestamp=" + now

	// 加密
	h := hmac.New(sha256.New, []byte(secretKey))
	h.Write([]byte(data))
	signature := hex.EncodeToString(h.Sum(nil))
	// 构造请求

	req, err = http.NewRequest("POST", apiUrl, strings.NewReader(data+"&signature="+signature))
	if err != nil {
		return nil, nil, err
	}
	// 添加头信息
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-MBX-APIKEY", apiKey)

	// 请求执行
	client = &http.Client{Timeout: 3 * time.Second}
	resp, err = client.Do(req)
	if err != nil {
		return nil, nil, err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(resp.Body)

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(string(b), err)
		return nil, nil, err
	}

	var o binanceOrder
	err = json.Unmarshal(b, &o)
	if err != nil {
		fmt.Println(string(b), err)
		return nil, nil, err
	}

	res = &binanceOrder{
		OrderId:       o.OrderId,
		ExecutedQty:   o.ExecutedQty,
		ClientOrderId: o.ClientOrderId,
		Symbol:        o.Symbol,
		AvgPrice:      o.AvgPrice,
		CumQuote:      o.CumQuote,
		Side:          o.Side,
		PositionSide:  o.PositionSide,
		ClosePosition: o.ClosePosition,
		Type:          o.Type,
	}

	if 0 >= res.OrderId {
		//fmt.Println(string(b))
		err = json.Unmarshal(b, &resOrderInfo)
		if err != nil {
			fmt.Println(string(b), err)
			return nil, nil, err
		}
	}

	return res, resOrderInfo, nil
}

type BinanceTraderDetailResp struct {
	Data *BinanceTraderDetailData
}

type BinanceTraderDetailData struct {
	MarginBalance string
	AumAmount     string
}

// 拉取交易员交易历史
func requestBinanceTraderDetail(portfolioId uint64) (string, string, error) {
	var (
		resp   *http.Response
		res    string
		resTwo string
		b      []byte
		err    error
		apiUrl = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/detail?portfolioId=" + strconv.FormatUint(portfolioId, 10)
	)

	// 构造请求
	resp, err = http.Get(apiUrl)
	if err != nil {
		return res, resTwo, err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(resp.Body)

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return res, resTwo, err
	}

	var l *BinanceTraderDetailResp
	err = json.Unmarshal(b, &l)
	if err != nil {
		fmt.Println(err)
		return res, resTwo, err
	}

	if nil == l.Data {
		return res, resTwo, nil
	}

	return l.Data.MarginBalance, l.Data.AumAmount, nil
}

type BinanceTraderExchangeInfoResp struct {
	Symbols []*BinanceExchangeInfoSymbol
}

type BinanceExchangeInfoSymbol struct {
	Symbol  string
	Filters []*BinanceExchangeInfoSymbolFilter
}

type BinanceExchangeInfoSymbolFilter struct {
	TickSize   string
	FilterType string
}

// 拉取币种信息
func requestBinanceExchangeInfo() ([]*BinanceExchangeInfoSymbol, error) {
	var (
		resp   *http.Response
		res    []*BinanceExchangeInfoSymbol
		b      []byte
		err    error
		apiUrl = "https://fapi.binance.com/fapi/v1/exchangeInfo"
	)

	// 构造请求
	resp, err = http.Get(apiUrl)
	if err != nil {
		return res, err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(resp.Body)

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return res, err
	}

	var l *BinanceTraderExchangeInfoResp
	err = json.Unmarshal(b, &l)
	if err != nil {
		fmt.Println(err)
		return res, err
	}

	if nil == l.Symbols || 0 >= len(l.Symbols) {
		return res, nil
	}

	return l.Symbols, nil
}

// 撤销订单信息
func requestBinanceDeleteOrder(symbol string, orderId string, apiKey string, secretKey string) (*binanceOrder, *orderInfo, error) {
	var (
		client       *http.Client
		req          *http.Request
		resp         *http.Response
		res          *binanceOrder
		resOrderInfo *orderInfo
		data         string
		b            []byte
		err          error
		apiUrl       = "https://fapi.binance.com/fapi/v1/order"
	)

	// 时间
	now := strconv.FormatInt(time.Now().UTC().UnixMilli(), 10)
	// 拼请求数据
	data = "symbol=" + symbol + "&orderId=" + orderId + "&timestamp=" + now

	// 加密
	h := hmac.New(sha256.New, []byte(secretKey))
	h.Write([]byte(data))
	signature := hex.EncodeToString(h.Sum(nil))
	// 构造请求

	req, err = http.NewRequest("DELETE", apiUrl, strings.NewReader(data+"&signature="+signature))
	if err != nil {
		return nil, nil, err
	}
	// 添加头信息
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-MBX-APIKEY", apiKey)

	// 请求执行
	client = &http.Client{Timeout: 3 * time.Second}
	resp, err = client.Do(req)
	if err != nil {
		return nil, nil, err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(resp.Body)

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(string(b), err)
		return nil, nil, err
	}

	var o binanceOrder
	err = json.Unmarshal(b, &o)
	if err != nil {
		fmt.Println(string(b), err)
		return nil, nil, err
	}

	res = &binanceOrder{
		OrderId:       o.OrderId,
		ExecutedQty:   o.ExecutedQty,
		ClientOrderId: o.ClientOrderId,
		Symbol:        o.Symbol,
		AvgPrice:      o.AvgPrice,
		CumQuote:      o.CumQuote,
		Side:          o.Side,
		PositionSide:  o.PositionSide,
		ClosePosition: o.ClosePosition,
		Type:          o.Type,
	}

	if 0 >= res.OrderId {
		//fmt.Println(string(b))
		err = json.Unmarshal(b, &resOrderInfo)
		if err != nil {
			fmt.Println(string(b), err)
			return nil, nil, err
		}
	}

	return res, resOrderInfo, nil
}

func requestBinanceOrderInfo(symbol string, orderId string, apiKey string, secretKey string) (*binanceOrder, error) {
	var (
		client *http.Client
		req    *http.Request
		resp   *http.Response
		res    *binanceOrder
		data   string
		b      []byte
		err    error
		apiUrl = "https://fapi.binance.com/fapi/v1/order"
	)

	// 时间
	now := strconv.FormatInt(time.Now().UTC().UnixMilli(), 10)
	// 拼请求数据
	data = "symbol=" + symbol + "&orderId=" + orderId + "&timestamp=" + now
	// 加密
	h := hmac.New(sha256.New, []byte(secretKey))
	h.Write([]byte(data))
	signature := hex.EncodeToString(h.Sum(nil))
	// 构造请求

	req, err = http.NewRequest("GET", apiUrl, strings.NewReader(data+"&signature="+signature))
	if err != nil {
		return nil, err
	}
	// 添加头信息
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-MBX-APIKEY", apiKey)

	// 请求执行
	client = &http.Client{Timeout: 3 * time.Second}
	resp, err = client.Do(req)
	if err != nil {
		return nil, err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(resp.Body)

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	var o binanceOrder
	err = json.Unmarshal(b, &o)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	res = &binanceOrder{
		OrderId:       o.OrderId,
		ExecutedQty:   o.ExecutedQty,
		ClientOrderId: o.ClientOrderId,
		Symbol:        o.Symbol,
		AvgPrice:      o.AvgPrice,
		CumQuote:      o.CumQuote,
		Side:          o.Side,
		PositionSide:  o.PositionSide,
		ClosePosition: o.ClosePosition,
		Type:          o.Type,
		Status:        o.Status,
	}

	return res, nil
}

// BinanceExchangeInfoResp 结构体表示 Binance 交易对信息的 API 响应
type BinanceExchangeInfoResp struct {
	Symbols []*BinanceSymbolInfo `json:"symbols"`
}

// BinanceSymbolInfo 结构体表示单个交易对的信息
type BinanceSymbolInfo struct {
	Symbol            string `json:"symbol"`
	Pair              string `json:"pair"`
	ContractType      string `json:"contractType"`
	Status            string `json:"status"`
	BaseAsset         string `json:"baseAsset"`
	QuoteAsset        string `json:"quoteAsset"`
	MarginAsset       string `json:"marginAsset"`
	PricePrecision    int    `json:"pricePrecision"`
	QuantityPrecision int    `json:"quantityPrecision"`
}

// 获取 Binance U 本位合约交易对信息
func getBinanceFuturesPairs() ([]*BinanceSymbolInfo, error) {
	apiUrl := "https://fapi.binance.com/fapi/v1/exchangeInfo"

	// 发送 HTTP GET 请求
	resp, err := http.Get(apiUrl)
	if err != nil {
		return nil, err
	}
	defer func() {
		if resp != nil && resp.Body != nil {
			err := resp.Body.Close()
			if err != nil {
				log.Println("关闭响应体错误：", err)
			}
		}
	}()

	// 读取响应体
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// 解析 JSON 响应
	var exchangeInfo *BinanceExchangeInfoResp
	err = json.Unmarshal(body, &exchangeInfo)
	if err != nil {
		return nil, err
	}

	return exchangeInfo.Symbols, nil
}

// GetGateContract 获取合约账号信息
func getGateContract() ([]gateapi.Contract, error) {
	client := gateapi.NewAPIClient(gateapi.NewConfiguration())
	// uncomment the next line if your are testing against testnet
	// client.ChangeBasePath("https://fx-api-testnet.gateio.ws/api/v4")
	ctx := context.WithValue(context.Background(),
		gateapi.ContextGateAPIV4,
		gateapi.GateAPIV4{},
	)

	result, _, err := client.FuturesApi.ListFuturesContracts(ctx, "usdt", &gateapi.ListFuturesContractsOpts{})
	if err != nil {
		var e gateapi.GateAPIError
		if errors.As(err, &e) {
			log.Println("gate api error: ", e.Error())
			return result, err
		}
	}

	return result, nil
}

// PlaceOrderGate places an order on the Gate.io API with dynamic parameters
func placeOrderGate(apiK, apiS, contract string, size int64, reduceOnly bool, autoSize string) (gateapi.FuturesOrder, error) {
	client := gateapi.NewAPIClient(gateapi.NewConfiguration())
	// uncomment the next line if your are testing against testnet
	// client.ChangeBasePath("https://fx-api-testnet.gateio.ws/api/v4")
	ctx := context.WithValue(context.Background(),
		gateapi.ContextGateAPIV4,
		gateapi.GateAPIV4{
			Key:    apiK,
			Secret: apiS,
		},
	)

	order := gateapi.FuturesOrder{
		Contract: contract,
		Size:     size,
		Tif:      "ioc",
		Price:    "0",
	}

	if autoSize != "" {
		order.AutoSize = autoSize
	}

	// 如果 reduceOnly 为 true，添加到请求数据中
	if reduceOnly {
		order.ReduceOnly = reduceOnly
	}

	result, _, err := client.FuturesApi.CreateFuturesOrder(ctx, "usdt", order)

	if err != nil {
		var e gateapi.GateAPIError
		if errors.As(err, &e) {
			log.Println("gate api error: ", e.Error())
			return result, err
		}
	}

	return result, nil
}

// PlaceOrderGate places an order on the Gate.io API with dynamic parameters
func placeLimitCloseOrderGate(apiK, apiS, contract string, price string, autoSize string) (gateapi.FuturesOrder, error) {
	client := gateapi.NewAPIClient(gateapi.NewConfiguration())
	// uncomment the next line if your are testing against testnet
	// client.ChangeBasePath("https://fx-api-testnet.gateio.ws/api/v4")
	ctx := context.WithValue(context.Background(),
		gateapi.ContextGateAPIV4,
		gateapi.GateAPIV4{
			Key:    apiK,
			Secret: apiS,
		},
	)

	order := gateapi.FuturesOrder{
		Contract:     contract,
		Size:         0,
		Price:        price,
		Tif:          "gtc",
		ReduceOnly:   true,
		AutoSize:     autoSize,
		IsReduceOnly: true,
		IsClose:      true,
	}

	result, _, err := client.FuturesApi.CreateFuturesOrder(ctx, "usdt", order)

	if err != nil {
		var e gateapi.GateAPIError
		if errors.As(err, &e) {
			log.Println("gate api error: ", e.Error())
			return result, err
		}
	}

	return result, nil
}

// PlaceOrderGate places an order on the Gate.io API with dynamic parameters
func removeLimitCloseOrderGate(apiK, apiS, orderId string) (gateapi.FuturesOrder, error) {
	client := gateapi.NewAPIClient(gateapi.NewConfiguration())
	// uncomment the next line if your are testing against testnet
	// client.ChangeBasePath("https://fx-api-testnet.gateio.ws/api/v4")
	ctx := context.WithValue(context.Background(),
		gateapi.ContextGateAPIV4,
		gateapi.GateAPIV4{
			Key:    apiK,
			Secret: apiS,
		},
	)

	result, _, err := client.FuturesApi.CancelFuturesOrder(ctx, "usdt", orderId)

	if err != nil {
		var e gateapi.GateAPIError
		if errors.As(err, &e) {
			log.Println("gate api error: ", e.Error())
			return result, err
		}
	}

	return result, nil
}

// PlaceOrderGate places an order on the Gate.io API with dynamic parameters
func getOrderGate(apiK, apiS, orderId string) (gateapi.FuturesOrder, error) {
	client := gateapi.NewAPIClient(gateapi.NewConfiguration())
	// uncomment the next line if your are testing against testnet
	// client.ChangeBasePath("https://fx-api-testnet.gateio.ws/api/v4")
	ctx := context.WithValue(context.Background(),
		gateapi.ContextGateAPIV4,
		gateapi.GateAPIV4{
			Key:    apiK,
			Secret: apiS,
		},
	)

	result, _, err := client.FuturesApi.GetFuturesOrder(ctx, "usdt", orderId)

	if err != nil {
		var e gateapi.GateAPIError
		if errors.As(err, &e) {
			log.Println("gate api error: ", e.Error())
			return result, err
		}
	}

	return result, nil
}

func placeLimitOrderGate(apiK, apiS, contract string, rule, timeLimit int32, price string, autoSize string) (gateapi.TriggerOrderResponse, error) {
	client := gateapi.NewAPIClient(gateapi.NewConfiguration())
	ctx := context.WithValue(context.Background(),
		gateapi.ContextGateAPIV4,
		gateapi.GateAPIV4{
			Key:    apiK,
			Secret: apiS,
		},
	)

	order := gateapi.FuturesPriceTriggeredOrder{
		Initial: gateapi.FuturesInitialOrder{
			Contract:     contract,
			Size:         0,
			Price:        price,
			Tif:          "gtc",
			ReduceOnly:   true,
			AutoSize:     autoSize,
			IsReduceOnly: true,
			IsClose:      true,
		},
		Trigger: gateapi.FuturesPriceTrigger{
			StrategyType: 0,
			PriceType:    0,
			Price:        price,
			Rule:         rule,
			Expiration:   timeLimit,
		},
	}

	result, _, err := client.FuturesApi.CreatePriceTriggeredOrder(ctx, "usdt", order)

	if err != nil {
		var e gateapi.GateAPIError
		if errors.As(err, &e) {
			log.Println("gate api error: ", e.Error())
			return result, err
		}
		return result, err
	}

	return result, nil
}
