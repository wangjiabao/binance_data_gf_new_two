package cmd

import (
	"binance_data_gf/internal/service"
	"context"
	"fmt"
	"github.com/gogf/gf/v2/os/gcmd"
	"github.com/gogf/gf/v2/os/gtimer"
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
				serviceBinanceTrader.PullAndOrderNewGuiTuPlay(ctx)
			}()

			// 阻塞
			select {}
		},
	}
)
