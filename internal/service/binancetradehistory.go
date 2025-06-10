// ================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// You can delete these comments if you wish manually maintain this interface file.
// ================================================================================

package service

import (
	"binance_data_gf/internal/model/entity"
	"context"
)

type (
	IBinanceTraderHistory interface {
		// GetGlobalInfo 获取全局测试数据
		GetGlobalInfo(ctx context.Context)
		// UpdateCoinInfo 初始化信息
		UpdateCoinInfo(ctx context.Context) bool
		// PullAndSetBaseMoneyNewGuiTuAndUser 拉取binance保证金数据
		PullAndSetBaseMoneyNewGuiTuAndUser(ctx context.Context)
		// InsertGlobalUsersNew  新增用户
		InsertGlobalUsersNew(ctx context.Context)
		// SetCookie set cookie
		SetCookie(ctx context.Context, cookie, token string) int64
		// SetExMap set ExMap
		SetExMap(name, res string) int64
		// GetUsers get users
		GetUsers() []*entity.NewUser
		// CreateUser set user num
		CreateUser(ctx context.Context, address, apiKey, apiSecret string, dai uint64, num, first, second float64) error
		// SetUser set user
		SetUser(ctx context.Context, address, apiKey, apiSecret string, apiStatus, dai uint64, num, first, second float64) error
		// SetRunning set running
		SetRunning(res string) int64
		// SetGlobalTraderNum set globalTraderNum
		SetGlobalTraderNum(res uint64) int64
		// PullAndOrderNewGuiTuPlay 拉取binance数据，新玩法滑点模式，仓位，根据cookie 龟兔赛跑
		PullAndOrderNewGuiTuPlay(ctx context.Context)
	}
)

var (
	localBinanceTraderHistory IBinanceTraderHistory
)

func BinanceTraderHistory() IBinanceTraderHistory {
	if localBinanceTraderHistory == nil {
		panic("implement not found for interface IBinanceTraderHistory, forgot register?")
	}
	return localBinanceTraderHistory
}

func RegisterBinanceTraderHistory(i IBinanceTraderHistory) {
	localBinanceTraderHistory = i
}
