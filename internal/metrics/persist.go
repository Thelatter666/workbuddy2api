// persist.go 统计采集器的落盘：把 hours/days 两个滚动窗口原子写入 JSON 文件，
// 进程启动时回放，使面板的「今日 / 每日 Credit、Tokens、请求数」跨重启连续。
//
// 刻意镜像 internal/pool/persist.go 的既有范式（dirty 标志 + 后台 flusher + 停机
// Flush + tmp→rename 原子写 + 失败节流日志），但**不复用其私有函数**——metrics 包
// 不反向依赖 pool（避免循环与耦合）。
//
// 只持久化两个时间环，life（进程累计）保持重启清零：面板「本进程请求」标签的语义
// 因此不变。槽位是稀疏存储的（仅非空槽落盘），文件很小。
package metrics

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

// flushInterval 后台落盘周期（package 级 var：测试可覆写，对齐 pool.flushInterval）。
var flushInterval = 5 * time.Second

// persistLogEvery 连续落盘失败每 N 次复报一条，避免磁盘异常时刷屏（同 pool 口径）。
const persistLogEvery = 100

// persistMetrics 落盘结构。SavedAt 供诊断文件新旧（当前不参与择新，保留语义扩展位）。
type persistMetrics struct {
	SavedAt time.Time       `json:"saved_at"`
	Hours   []persistBucket `json:"hours,omitempty"`
	Days    []persistBucket `json:"days,omitempty"`
}

// persistBucket 单槽序列化形态。index 是周期序号（回放时按 idx%N 落位），
// withDims 不入盘（由环的构造参数决定，回放时按目标环重建）。
type persistBucket struct {
	Index            int64            `json:"index"`
	Requests         int64            `json:"requests,omitempty"`
	OK               int64            `json:"ok,omitempty"`
	Fail             int64            `json:"fail,omitempty"`
	PromptTokens     int64            `json:"prompt_tokens,omitempty"`
	CompletionTokens int64            `json:"completion_tokens,omitempty"`
	Tokens           int64            `json:"tokens,omitempty"`
	Credit           float64          `json:"credit,omitempty"`
	TTFBSumNs        int64            `json:"ttfb_sum_ns,omitempty"`
	TTFBCount        int64            `json:"ttfb_count,omitempty"`
	TTFBMaxNs        int64            `json:"ttfb_max_ns,omitempty"`
	ByUID            map[string]Dim   `json:"by_uid,omitempty"`
	ByModel          map[string]Dim   `json:"by_model,omitempty"`
	ByStatus         map[string]int64 `json:"by_status,omitempty"`
	// Balances 当日收盘余额快照（uid → 剩余积分）。只有日环会写。
	Balances map[string]int64 `json:"balances,omitempty"`
}

// persistLedger 长期日台账落盘结构。独立文件：台账每天至多写一次，
// 与 5s 一次的热数据分开，长期保留年限就不会放大热路径的写盘量。
type persistLedger struct {
	SavedAt time.Time    `json:"saved_at"`
	Days    []*LedgerDay `json:"days,omitempty"`
}

// Persistent 报告热数据是否落盘（stateFp 非空）。
// stateFp 仅在构造期赋值一次、之后不再变更，故无需持锁读取。
func (c *Collector) Persistent() bool { return c.stateFp != "" }

// LedgerPersistent 报告长期台账是否落盘（ledgerFp 非空）。
func (c *Collector) LedgerPersistent() bool { return c.ledgerFp != "" }

// NewWithState 构造带落盘的热数据采集器（等价于不带长期台账）。
func NewWithState(stateFp string) *Collector { return NewWithStateLedger(stateFp, "") }

// NewWithStateLedger 构造带落盘的采集器：stateFp 落小时/日环（热），ledgerFp 落长期
// 日台账（冷，每天至多一次）。空路径各自退化为「不落该项」，两者都空则等价 New()。
func NewWithStateLedger(stateFp, ledgerFp string) *Collector {
	c := New()
	c.stateFp, c.ledgerFp = stateFp, ledgerFp
	if stateFp == "" && ledgerFp == "" {
		return c
	}
	c.load()
	c.startFlusher()
	return c
}

// startFlusher 后台按 flushInterval 把 dirty 状态落盘。
// interval 在起 goroutine 前同步读取，避免测试恢复 flushInterval 时的写竞争（同 pool）。
func (c *Collector) startFlusher() {
	interval := flushInterval
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for range t.C {
			c.Flush()
		}
	}()
}

// Flush 同步落盘（幂等：无变更不写盘）。供请求路径外的 ticker 与进程退出前调用。
// 写失败时保留 dirty，下一次 Flush 自动重试（一次磁盘抖动不该永久丢数据）。
// 落盘前先采一次余额、沉淀一次台账：保证长时间没人打开面板时长期数据也在攒。
func (c *Collector) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pollBalancesLocked()
	c.absorbLedgerLocked()
	if c.stateFp != "" && c.dirty.Load() {
		if c.saveLocked() {
			c.dirty.Store(false)
		}
	}
	if c.ledgerFp != "" && c.ledgerDirty.Load() {
		if c.saveLedgerLocked() {
			c.ledgerDirty.Store(false)
		}
	}
}

// saveLocked 序列化两个环并原子写入。调用方必须已持 c.mu。
// 返回是否成功；失败时调用方应保留 dirty 以便重试。
func (c *Collector) saveLocked() bool {
	if c.stateFp == "" {
		return true
	}
	pm := persistMetrics{
		SavedAt: time.Now(),
		Hours:   dumpRing(c.hours),
		Days:    dumpRing(c.days),
	}
	if !c.writeJSONAtomic(c.stateFp, pm) {
		return false
	}
	if c.persistFails > 0 {
		log.Printf("[metrics] 落盘恢复（此前连续失败 %d 次）", c.persistFails)
		c.persistFails = 0
	}
	return true
}

// saveLedgerLocked 原子写长期台账。调用方必须已持 c.mu。
func (c *Collector) saveLedgerLocked() bool {
	if c.ledgerFp == "" {
		return true
	}
	return c.writeJSONAtomic(c.ledgerFp, persistLedger{SavedAt: time.Now(), Days: c.ledger})
}

// writeJSONAtomic 序列化 → 建目录 → 写临时文件 → rename 的原子落盘（两个文件共用）。
func (c *Collector) writeJSONAtomic(path string, v any) bool {
	raw, err := json.Marshal(v)
	if err != nil {
		c.notePersistFail(path, err)
		return false
	}
	if dir := filepath.Dir(path); dir != "" {
		_ = os.MkdirAll(dir, 0o755)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		c.notePersistFail(path, err)
		return false
	}
	if err := os.Rename(tmp, path); err != nil {
		c.notePersistFail(path, err)
		return false
	}
	return true
}

// load 回放落盘文件。文件不存在/损坏/字段缺失一律静默跳过（对齐 pool.load 的容错），
// 采集器退化为空历史——历史丢失不应阻止网关启动。
func (c *Collector) load() {
	if c.stateFp != "" {
		if raw, err := os.ReadFile(c.stateFp); err == nil {
			var pm persistMetrics
			if json.Unmarshal(raw, &pm) == nil {
				restoreRing(c.hours, pm.Hours)
				restoreRing(c.days, pm.Days)
			}
		}
	}
	if c.ledgerFp != "" {
		if raw, err := os.ReadFile(c.ledgerFp); err == nil {
			var pl persistLedger
			if json.Unmarshal(raw, &pl) == nil {
				for _, d := range pl.Days {
					if d == nil || d.Index < 0 {
						continue
					}
					if c.ledgerIdx[d.Index] == nil {
						c.insertLedgerLocked(d)
					}
				}
			}
		}
	}
}

// dumpRing 导出环内非空槽（稀疏）。life 不在此列（不落盘）。
func dumpRing(r *ring) []persistBucket {
	var out []persistBucket
	for i := range r.slots {
		b := &r.slots[i]
		if b.index < 0 {
			continue // 空槽
		}
		out = append(out, persistBucket{
			Index:            b.index,
			Requests:         b.Requests,
			OK:               b.OK,
			Fail:             b.Fail,
			PromptTokens:     b.PromptTokens,
			CompletionTokens: b.CompletionTokens,
			Tokens:           b.Tokens,
			Credit:           b.Credit,
			TTFBSumNs:        b.ttfbSumNs,
			TTFBCount:        b.ttfbCount,
			TTFBMaxNs:        b.ttfbMaxNs,
			ByUID:            copyDims(b.byUID),
			ByModel:          copyDims(b.byModel),
			ByStatus:         copyStatus(b.byStatus),
			Balances:         copyInt64Map(b.balances),
		})
	}
	return out
}

// copyInt64Map 复制快照类小 map（nil 透传，保持 omitempty 语义）。
func copyInt64Map(m map[string]int64) map[string]int64 {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// restoreRing 把落盘槽位按 idx%len(slots) 放回环中。越界/重复槽按后者覆盖处理，
// 不 panic（文件被手改坏也只损失统计，不影响进程）。
func restoreRing(r *ring, buckets []persistBucket) {
	n := int64(len(r.slots))
	for _, pb := range buckets {
		if pb.Index < 0 {
			continue
		}
		pos := ((pb.Index % n) + n) % n
		b := &r.slots[pos]
		b.index = pb.Index
		b.Requests = pb.Requests
		b.OK = pb.OK
		b.Fail = pb.Fail
		b.PromptTokens = pb.PromptTokens
		b.CompletionTokens = pb.CompletionTokens
		b.Tokens = pb.Tokens
		b.Credit = pb.Credit
		b.ttfbSumNs = pb.TTFBSumNs
		b.ttfbCount = pb.TTFBCount
		b.ttfbMaxNs = pb.TTFBMaxNs
		b.byUID = restoreDims(pb.ByUID)
		b.byModel = restoreDims(pb.ByModel)
		b.byStatus = pb.ByStatus
		b.balances = copyInt64Map(pb.Balances)
		// withDims 保持不变（由环构造参数决定，不入盘）。
	}
}

func restoreDims(m map[string]Dim) map[string]*Dim {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]*Dim, len(m))
	for k, v := range m {
		d := v
		out[k] = &d
	}
	return out
}

// notePersistFail 记录一次落盘失败并按节流规则打日志：首败详报（含路径与目录信息），
// 随后每 persistLogEvery 次复报一条。与 pool.notePersistFail 同风格，但不依赖其私有件。
func (c *Collector) notePersistFail(path string, err error) {
	if c.persistFails == 0 {
		log.Printf("WARN: [metrics] 落盘失败（首次详报）: path=%s err=%v %s",
			path, err, persistFailDiag(path))
	} else if c.persistFails%persistLogEvery == 0 {
		log.Printf("ERR: [metrics] 连续落盘失败 %d 次: path=%s err=%v",
			c.persistFails, path, err)
	}
	c.persistFails++
}

// persistFailDiag 收集落盘失败时的目录诊断（存在性/权限），供首错定位 bind mount 属主问题。
func persistFailDiag(stateFp string) string {
	dir := filepath.Dir(stateFp)
	info, statErr := os.Stat(dir)
	if statErr != nil {
		return fmt.Sprintf("stat_dir=%s err=%v（目录不存在或不可访问）", dir, statErr)
	}
	return fmt.Sprintf("dir=%s mode=%o proc_uid=%d", dir, info.Mode().Perm(), os.Getuid())
}
