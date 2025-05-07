// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package do

import (
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
)

// NewUser is the golang structure of table new_user for DAO operations like Where/Data.
type NewUser struct {
	g.Meta              `orm:"table:new_user, do:true"`
	Id                  interface{} // 用户id
	Address             interface{} // 用户address
	ApiStatus           interface{} // api的可用状态：不可用2
	ApiKey              interface{} // 用户币安apikey
	ApiSecret           interface{} // 用户币安apisecret
	OpenStatus          interface{} //
	CreatedAt           *gtime.Time //
	UpdatedAt           *gtime.Time //
	NeedInit            interface{} //
	Num                 interface{} //
	Plat                interface{} //
	Dai                 interface{} //
	Ip                  interface{} //
	BindTraderStatus    interface{} //
	BindTraderStatusTfi interface{} //
	UseNewSystem        interface{} //
	OrderType           interface{} //
	BinanceId           interface{} //
	First               interface{} //
	Second              interface{} //
	IsDai               interface{} //
	Three               interface{} //
	Four                interface{} //
}
