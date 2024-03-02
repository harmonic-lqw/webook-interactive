package dao

import (
	"context"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

type InteractiveDAO interface {
	IncrReadCnt(ctx context.Context, biz string, bizId int64) error
	BatchIncrReadCnt(ctx context.Context, bizs []string, bizIds []int64) error
	InsertLikeInfo(ctx context.Context, biz string, bizId int64, uid int64) error
	DeleteLikeInfo(ctx context.Context, biz string, bizId int64, uid int64) error
	InsertCollectionBiz(ctx context.Context, biz string, bizId int64, cid int64, uid int64) error
	GetLikeInfo(ctx context.Context, biz string, bizId int64, uid int64) (UserLikeBiz, error)
	GetCollectInfo(ctx context.Context, biz string, bizId int64, uid int64) (UserCollectionBiz, error)
	Get(ctx context.Context, biz string, bizId int64) (Interactive, error)
	GetByIds(ctx context.Context, biz string, bizIds []int64) ([]Interactive, error)
}

type GORMInteractiveDAO struct {
	db *gorm.DB
}

func (g *GORMInteractiveDAO) GetByIds(ctx context.Context, biz string, bizIds []int64) ([]Interactive, error) {
	var res []Interactive
	err := g.db.WithContext(ctx).
		Where("biz_id IN ? AND biz = ?", bizIds, biz).
		First(&res).
		Error
	return res, err
}

func (g *GORMInteractiveDAO) Get(ctx context.Context, biz string, bizId int64) (Interactive, error) {
	var res Interactive
	err := g.db.WithContext(ctx).
		Where("biz_id = ? AND biz = ?", bizId, biz).
		First(&res).
		Error
	return res, err
}

func (g *GORMInteractiveDAO) GetCollectInfo(ctx context.Context, biz string, bizId int64, uid int64) (UserCollectionBiz, error) {
	var res UserCollectionBiz
	err := g.db.WithContext(ctx).
		Where("uid = ? AND biz_id = ? AND biz = ?", uid, bizId, biz).
		First(&res).Error
	return res, err
}

func (g *GORMInteractiveDAO) GetLikeInfo(ctx context.Context, biz string, bizId int64, uid int64) (UserLikeBiz, error) {
	var res UserLikeBiz
	err := g.db.WithContext(ctx).
		Where("uid = ? AND biz_id = ? AND biz = ? AND status = ?", uid, bizId, biz, 1).
		First(&res).Error
	return res, err
}

func (g *GORMInteractiveDAO) InsertCollectionBiz(ctx context.Context, biz string, bizId int64, cid int64, uid int64) error {
	now := time.Now().UnixMilli()
	return g.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Create(&UserCollectionBiz{
			Biz:   biz,
			BizId: bizId,
			Cid:   cid,
			Uid:   uid,
			Ctime: now,
			Utime: now,
		}).Error
		if err != nil {
			return err
		}
		// 更新收藏计数
		return tx.WithContext(ctx).
			Clauses(clause.OnConflict{
				DoUpdates: clause.Assignments(map[string]interface{}{
					"collect_cnt": gorm.Expr("`collect_cnt` + 1"), // 在数据库层面保证线程安全
					"utime":       now,
				}),
			}).
			Create(&Interactive{
				Biz:        biz,
				BizId:      bizId,
				CollectCnt: 1,
				Ctime:      now,
				Utime:      now,
			}).Error
	})
}

func (g *GORMInteractiveDAO) InsertLikeInfo(ctx context.Context, biz string, bizId int64, uid int64) error {
	// 开启事务，因为既要实现点赞功能，也要实现该文章点赞数的递增
	now := time.Now().UnixMilli()
	return g.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Clauses(clause.OnConflict{
			DoUpdates: clause.Assignments(map[string]interface{}{
				"utime":  now,
				"status": 1,
			}),
		}).Create(&UserLikeBiz{
			Uid:    uid,
			Biz:    biz,
			BizId:  bizId,
			Status: 1,
			Utime:  now,
			Ctime:  now,
		}).Error
		if err != nil {
			return err
		}
		// 更新文章点赞计数
		return tx.WithContext(ctx).
			Clauses(clause.OnConflict{
				DoUpdates: clause.Assignments(map[string]interface{}{
					"like_cnt": gorm.Expr("`like_cnt` + 1"), // 在数据库层面保证线程安全
					"utime":    now,
				}),
			}).
			Create(&Interactive{
				Biz:     biz,
				BizId:   bizId,
				LikeCnt: 1,
				Ctime:   now,
				Utime:   now,
			}).Error
	})
}

func (g *GORMInteractiveDAO) DeleteLikeInfo(ctx context.Context, biz string, bizId int64, uid int64) error {
	now := time.Now().UnixMilli()
	return g.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Model(&UserLikeBiz{}).
			Where("uid = ? AND biz_id = ? AND biz = ?", uid, bizId, biz).
			Updates(map[string]interface{}{
				"utime":  now,
				"status": 0,
			}).Error
		if err != nil {
			return err
		}
		// 更新文章点赞计数
		return tx.Model(&Interactive{}).
			Where("biz_id = ? AND biz = ?", bizId, biz).
			Updates(map[string]interface{}{
				"like_cnt": gorm.Expr("`like_cnt` - 1"),
				"utime":    now,
			}).Error

	})
}

func NewGORMInteractiveDAO(db *gorm.DB) InteractiveDAO {
	return &GORMInteractiveDAO{
		db: db,
	}
}

func (g *GORMInteractiveDAO) BatchIncrReadCnt(ctx context.Context, bizs []string, bizIds []int64) error {
	// 亮点：这里利用了一个事务只提交一次的特性，复用 IncrReadCnt
	// 多次插入一次提交比多次插入多次提交效率高很多
	return g.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		newDAO := NewGORMInteractiveDAO(tx)
		for i := 0; i < len(bizs); i++ {
			err := newDAO.IncrReadCnt(ctx, bizs[i], bizIds[i])
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (g *GORMInteractiveDAO) IncrReadCnt(ctx context.Context, biz string, bizId int64) error {
	now := time.Now().UnixMilli()
	return g.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			DoUpdates: clause.Assignments(map[string]interface{}{
				"read_cnt": gorm.Expr("`read_cnt` + 1"), // 在数据库层面保证线程安全
				"utime":    now,
			}),
		}).
		Create(&Interactive{
			Biz:     biz,
			BizId:   bizId,
			ReadCnt: 1,
			Ctime:   now,
			Utime:   now,
		}).Error
}

// UserLikeBiz 判断该用户是否点赞该文章，是从这张表里面查询的
type UserLikeBiz struct {
	Id int64 `gorm:"primaryKey, autoIncrement"`

	Uid   int64  `gorm:"uniqueIndex:uid_biz_type_id"`
	BizId int64  `gorm:"uniqueIndex:uid_biz_type_id"`
	Biz   string `gorm:"type=varchar(128), uniqueIndex:uid_biz_type_id"`
	// 标记软删除
	Status uint8
	Utime  int64
	Ctime  int64
}

// UserCollectionBiz 判断该用户是否收藏该文章，是从这张表里面查询的
type UserCollectionBiz struct {
	Id int64 `gorm:"primaryKey, autoIncrement"`

	Uid   int64  `gorm:"uniqueIndex:uid_biz_type_id"`
	BizId int64  `gorm:"uniqueIndex:uid_biz_type_id"`
	Biz   string `gorm:"type=varchar(128), uniqueIndex:uid_biz_type_id"`
	// 收藏夹 ID
	Cid   int64 `gorm:"index"`
	Utime int64
	Ctime int64
}

type Interactive struct {
	Id int64 `gorm:"primaryKey, autoIncrement"`

	// 创建联合索引 <bizid, biz>
	BizId int64  `gorm:"uniqueIndex:biz_type_id"`
	Biz   string `gorm:"type=varchar(128), uniqueIndex:biz_type_id"`

	ReadCnt    int64
	LikeCnt    int64
	CollectCnt int64
	Utime      int64
	Ctime      int64
}
