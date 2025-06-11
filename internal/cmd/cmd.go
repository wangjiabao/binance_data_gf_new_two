package cmd

import (
	"binance_data_gf/internal/service"
	"context"
	"fmt"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/net/ghttp"
	"github.com/gogf/gf/v2/os/gcmd"
	"github.com/gogf/gf/v2/os/gtimer"
	"log"
	"strconv"
	"time"
)

var (
	Main = &gcmd.Command{
		Name: "main",
	}

	// TraderGuiNew 监听系统中指定的交易员-龟兔赛跑
	TraderGuiNew = &gcmd.Command{
		Name:  "traderGuiNew",
		Brief: "listen trader",
		Func: func(ctx context.Context, parser *gcmd.Parser) (err error) {
			serviceBinanceTrader := service.BinanceTraderHistory()

			// 初始化根据数据库现有人
			if !serviceBinanceTrader.UpdateCoinInfo(ctx) {
				fmt.Println("初始化币种失败，fail")
				return nil
			}
			fmt.Println("初始化币种成功，ok")

			// 拉龟兔的保证金
			serviceBinanceTrader.PullAndSetBaseMoneyNewGuiTuAndUser(ctx)

			// 10秒/次，拉取保证金
			handle := func(ctx context.Context) {
				serviceBinanceTrader.PullAndSetBaseMoneyNewGuiTuAndUser(ctx)
			}
			gtimer.AddSingleton(ctx, time.Second*10, handle)

			// 30秒/次，加新人
			handle2 := func(ctx context.Context) {
				serviceBinanceTrader.InsertGlobalUsersNew(ctx)
			}
			gtimer.AddSingleton(ctx, time.Second*30, handle2)

			// 300秒/次，币种信息
			handle3 := func(ctx context.Context) {
				serviceBinanceTrader.UpdateCoinInfo(ctx)
			}
			gtimer.AddSingleton(ctx, time.Second*300, handle3)

			// 任务1 同步订单
			go func() {
				for {
					serviceBinanceTrader.PullAndOrderNewGuiTuPlay(ctx)
					time.Sleep(10 * time.Second)
				}
			}()

			s := g.Server()
			// 使用 CORS 中间件（全局）

			s.Group("/api", func(group *ghttp.RouterGroup) {
				group.Middleware(func(r *ghttp.Request) {
					r.Response.Header().Set("Access-Control-Allow-Origin", "*")
					r.Response.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
					r.Response.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

					// OPTIONS 请求直接返回
					if r.Method == "OPTIONS" {
						r.Exit()
					}
					r.Middleware.Next()
				})

				// 用户设置
				group.POST("/create/user", func(r *ghttp.Request) {
					var (
						parseErr error
						setErr   error
						num      float64
						first    uint64
						second   float64
						dai      uint64
					)
					dai, parseErr = strconv.ParseUint(r.PostFormValue("dai"), 10, 64)
					if nil != parseErr {
						r.Response.WriteJson(g.Map{
							"code": -1,
						})

						return
					}

					num, parseErr = strconv.ParseFloat(r.PostFormValue("num"), 64)
					if nil != parseErr || 0 >= num {
						r.Response.WriteJson(g.Map{
							"code": -1,
						})

						return
					}

					first, parseErr = strconv.ParseUint(r.PostFormValue("first"), 10, 64)
					if nil != parseErr {
						r.Response.WriteJson(g.Map{
							"code": -1,
						})

						return
					}

					second, parseErr = strconv.ParseFloat(r.PostFormValue("second"), 64)
					if nil != parseErr {
						r.Response.WriteJson(g.Map{
							"code": -1,
						})

						return
					}

					setErr = serviceBinanceTrader.CreateUser(
						ctx,
						r.PostFormValue("address"),
						r.PostFormValue("apiKey"),
						r.PostFormValue("apiSecret"),
						dai,
						num,
						float64(first),
						second,
					)
					if nil != setErr {
						r.Response.WriteJson(g.Map{
							"code": -2,
						})

						return
					}

					r.Response.WriteJson(g.Map{
						"code": 1,
					})

					return
				})

				group.POST("/set/user", func(r *ghttp.Request) {
					var (
						parseErr  error
						setErr    error
						num       float64
						first     uint64
						second    float64
						dai       uint64
						apiStatus uint64
					)
					dai, parseErr = strconv.ParseUint(r.PostFormValue("dai"), 10, 64)
					if nil != parseErr {
						r.Response.WriteJson(g.Map{
							"code": -1,
						})

						return
					}

					apiStatus, parseErr = strconv.ParseUint(r.PostFormValue("apiStatus"), 10, 64)
					if nil != parseErr {
						r.Response.WriteJson(g.Map{
							"code": -1,
						})

						return
					}

					num, parseErr = strconv.ParseFloat(r.PostFormValue("num"), 64)
					if nil != parseErr || 0 >= num {
						r.Response.WriteJson(g.Map{
							"code": -1,
						})

						return
					}

					first, parseErr = strconv.ParseUint(r.PostFormValue("first"), 10, 64)
					if nil != parseErr {
						r.Response.WriteJson(g.Map{
							"code": -1,
						})

						return
					}

					second, parseErr = strconv.ParseFloat(r.PostFormValue("second"), 64)
					if nil != parseErr {
						r.Response.WriteJson(g.Map{
							"code": -1,
						})

						return
					}

					setErr = serviceBinanceTrader.SetUser(
						ctx,
						r.PostFormValue("address"),
						r.PostFormValue("apiKey"),
						r.PostFormValue("apiSecret"),
						apiStatus,
						dai,
						num,
						float64(first),
						second,
					)
					if nil != setErr {
						r.Response.WriteJson(g.Map{
							"code": -2,
						})

						return
					}

					r.Response.WriteJson(g.Map{
						"code": 1,
					})

					return
				})

				group.GET("/users", func(r *ghttp.Request) {

					res := serviceBinanceTrader.GetUsers()
					responseData := make([]*g.Map, 0)
					for _, v := range res {
						responseData = append(responseData, &g.Map{
							"address":   v.Address,
							"apiKey":    v.ApiKey,
							"apiSecret": v.ApiSecret,
							"dai":       v.Dai,
							"num":       v.Num,
							"first":     v.First,
							"second":    v.Second,
							"apiStatus": v.ApiStatus,
						})
					}

					r.Response.WriteJson(responseData)
				})

				// cookie设置
				group.POST("/set/cookie", func(r *ghttp.Request) {
					r.Response.WriteJson(g.Map{
						"code": serviceBinanceTrader.SetCookie(ctx, r.PostFormValue("cookie"), r.PostFormValue("token")),
					})

					return
				})

				group.GET("/cookie", func(r *ghttp.Request) {
					cookie, token, isOpen := serviceBinanceTrader.GetCookie(ctx)
					r.Response.WriteJson(g.Map{
						"cookie": cookie,
						"token":  token,
						"isOpen": isOpen,
					})
				})

				// trader num设置
				group.POST("/set/trader_num", func(r *ghttp.Request) {
					num, parseErr := strconv.ParseUint(r.PostFormValue("num"), 10, 64)
					if nil != parseErr {
						r.Response.WriteJson(g.Map{
							"code": -1,
						})

						return
					}

					serviceBinanceTrader.SetRunning("stop")
					time.Sleep(3 * time.Second)

					serviceBinanceTrader.SetGlobalTraderNum(num)
					serviceBinanceTrader.PullAndSetBaseMoneyNewGuiTuAndUser(ctx)

					time.Sleep(3 * time.Second)
					serviceBinanceTrader.SetRunning("running")

					log.Println("更新交易员完成")

					r.Response.WriteJson(g.Map{
						"code": 1,
					})

					return
				})

				group.GET("/trader_num", func(r *ghttp.Request) {
					num := serviceBinanceTrader.GetGlobalTraderNum()
					r.Response.WriteJson(g.Map{
						"num": num,
					})
				})

				// 排除币种设置
				group.POST("/set/ex_map", func(r *ghttp.Request) {
					r.Response.WriteJson(g.Map{
						"code": serviceBinanceTrader.SetExMap(r.PostFormValue("name"), r.PostFormValue("res")),
					})

					return
				})

				group.GET("/ex_map", func(r *ghttp.Request) {
					exMap := serviceBinanceTrader.GetExMap()

					res := make([]*g.Map, 0)
					for k, v := range exMap {
						res = append(res, &g.Map{
							k: v,
						})
					}

					r.Response.WriteJson(res)
				})
			})

			s.SetPort(80)
			s.Run()

			// 阻塞
			select {}
		},
	}
)
