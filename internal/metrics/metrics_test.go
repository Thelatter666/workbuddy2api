// metrics_test.go 采集器分桶/环滚动/并发/快照结构测试。
package metrics

import (
	"sync"
	"testing"
	"time"
)

// at 构造一个确定时刻（UTC），便于断言归属周期。
func at(y int, mo time.Month, d, h int) time.Time {
	return time.Date(y, mo, d, h, 30, 0, 0, time.UTC)
}

func sample(t time.Time, uid, model string, status, completion, prompt int, credit float64, hasCredit bool) Sample {
	return Sample{
		Start: t, Model: model, Mode: "stream", UID: uid, Status: status,
		Tokens: completion, PromptTokens: prompt, Credit: credit, HasCredit: hasCredit,
		TTFB: 100 * time.Millisecond,
	}
}

func TestRecordAggregatesScalars(t *testing.T) {
	c := New()
	ts := at(2026, time.September, 25, 10)
	c.Record(sample(ts, "u1", "glm-5.2", 200, 100, 50, 1.5, true))
	c.Record(sample(ts, "u1", "glm-5.2", 200, 200, 30, 0.5, true))
	c.Record(sample(ts, "u2", "glm-5.2", 429, -1, 0, 0, false)) // usage 缺失 + 失败

	s := c.Snapshot()
	if s.Total.Requests != 3 || s.Total.OK != 2 || s.Total.Fail != 1 {
		t.Fatalf("total req/ok/fail = %d/%d/%d, want 3/2/1", s.Total.Requests, s.Total.OK, s.Total.Fail)
	}
	// 缺失 usage 的请求不得反噬 token 合计：100+50 + 200+30 = 380。
	if s.Total.Tokens != 380 {
		t.Errorf("total tokens = %d, want 380", s.Total.Tokens)
	}
	if s.Total.PromptTokens != 80 || s.Total.CompletionTokens != 300 {
		t.Errorf("prompt/completion = %d/%d, want 80/300", s.Total.PromptTokens, s.Total.CompletionTokens)
	}
	if s.Total.Credit != 2.0 {
		t.Errorf("credit = %v, want 2.0", s.Total.Credit)
	}
	if s.ByUID["u1"].Requests != 2 || s.ByUID["u2"].Requests != 1 {
		t.Errorf("by_uid 分组错误: %+v", s.ByUID)
	}
	if s.ByStatus["200"] != 2 || s.ByStatus["429"] != 1 {
		t.Errorf("by_status 分组错误: %+v", s.ByStatus)
	}
}

func TestCreditMissingNotCountedAsZero(t *testing.T) {
	c := New()
	ts := at(2026, time.September, 25, 10)
	c.Record(sample(ts, "u1", "m", 200, 10, 5, 0, false)) // 缺失 credit（HasCredit=false）
	c.Record(sample(ts, "u1", "m", 200, 10, 5, 0, true))  // 显式 credit:0（合法免费观测）

	s := c.Snapshot()
	if s.Total.Requests != 2 {
		t.Fatalf("requests = %d, want 2", s.Total.Requests)
	}
	if s.Total.OK != 2 {
		t.Errorf("ok = %d, want 2", s.Total.OK)
	}
}

func TestHourAndDayBucketing(t *testing.T) {
	c := New()
	// 以真实 now 为锚（Snapshot 的环窗口终点是 now，未来时刻不在窗口内）：
	// 本小时 2 个请求、上一小时 1 个。槽位索引从末尾数：[-1]=本小时，[-2]=上一小时。
	now := time.Now()
	prev := now.Add(-time.Hour)
	c.Record(sample(now, "u1", "m", 200, 1, 1, 0, false))
	c.Record(sample(now, "u2", "m", 200, 1, 1, 0, false))
	c.Record(sample(prev, "u1", "m", 200, 1, 1, 0, false))

	s := c.Snapshot()
	if len(s.Hours) != HourSlots || len(s.Days) != DaySlots {
		t.Fatalf("hours=%d(期望%d) days=%d(期望%d)", len(s.Hours), HourSlots, len(s.Days), DaySlots)
	}
	if got := s.Hours[len(s.Hours)-1].Requests; got != 2 {
		t.Errorf("本小时 requests = %d, want 2", got)
	}
	if got := s.Hours[len(s.Hours)-2].Requests; got != 1 {
		t.Errorf("上一小时 requests = %d, want 1", got)
	}
	// 日环：三个请求都落在同一天（除跨 UTC+8 零点那一刻），合计必为 3。
	var dayReqs int64
	for _, d := range s.Days {
		dayReqs += d.Requests
	}
	if dayReqs != 3 {
		t.Errorf("日环合计 = %d, want 3", dayReqs)
	}
}

func TestRingSlotReuseExpiresOldData(t *testing.T) {
	c := New()
	// 记录在当前小时；再记录在 HourSlots 之前（两者物理落在同一环槽）。
	// 后写必须重置同槽，丢弃旧数据——但必须在“过去”写，才能反映真实的回绕覆盖。
	now := time.Now()
	old := now.Add(-time.Duration(HourSlots) * time.Hour)
	c.Record(sample(old, "u1", "m", 200, 1000, 1000, 99, true)) // 大数：若残留必然可见
	c.Record(sample(now, "u2", "m", 200, 1, 1, 0, false))

	s := c.Snapshot()
	last := s.Hours[len(s.Hours)-1]
	if last.Requests != 1 || last.Tokens != 2 {
		t.Fatalf("回绕槽未被重置: req=%d tok=%d（期望 1/2）", last.Requests, last.Tokens)
	}
	// 进程累计 life 不受环回绕影响，仍是 2 次。
	if s.Total.Requests != 2 {
		t.Errorf("life 累计 = %d, want 2", s.Total.Requests)
	}
}

func TestSnapshotTimesAscendingAndCorrect(t *testing.T) {
	c := New()
	s := c.Snapshot()
	if len(s.Hours) != HourSlots {
		t.Fatalf("hours len = %d", len(s.Hours))
	}
	for i := 1; i < len(s.Hours); i++ {
		prev, _ := time.Parse(time.RFC3339, s.Hours[i-1].Ts)
		cur, _ := time.Parse(time.RFC3339, s.Hours[i].Ts)
		if !cur.After(prev) {
			t.Fatalf("小时环时间未升序: %s -> %s", s.Hours[i-1].Ts, s.Hours[i].Ts)
		}
		if cur.Sub(prev) != time.Hour {
			t.Fatalf("小时环步长 = %v, want 1h", cur.Sub(prev))
		}
	}
	for i := 1; i < len(s.Days); i++ {
		prev, _ := time.Parse(time.RFC3339, s.Days[i-1].Ts)
		cur, _ := time.Parse(time.RFC3339, s.Days[i].Ts)
		if cur.Sub(prev) != 24*time.Hour {
			t.Fatalf("日环步长 = %v, want 24h", cur.Sub(prev))
		}
	}
}

func TestTopModelsRanked(t *testing.T) {
	c := New()
	ts := at(2026, time.September, 25, 10)
	c.Record(sample(ts, "u1", "a", 200, 1, 1, 0, false))
	c.Record(sample(ts, "u1", "b", 200, 1, 1, 0, false))
	c.Record(sample(ts, "u1", "b", 200, 1, 1, 0, false))
	c.Record(sample(ts, "u1", "b", 200, 1, 1, 0, false))
	s := c.Snapshot()
	if len(s.TopModels) != 2 || s.TopModels[0].Model != "b" || s.TopModels[0].Requests != 3 {
		t.Fatalf("排行错误: %+v", s.TopModels)
	}
}

func TestConcurrentRecord(t *testing.T) {
	c := New()
	const goroutines, per = 8, 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ts := time.Now()
			for i := 0; i < per; i++ {
				c.Record(sample(ts, "u1", "m", 200, 1, 1, 0.5, true))
			}
		}()
	}
	wg.Wait()
	s := c.Snapshot()
	if s.Total.Requests != goroutines*per {
		t.Fatalf("并发累计 = %d, want %d", s.Total.Requests, goroutines*per)
	}
	if s.Total.Credit != float64(goroutines*per)*0.5 {
		t.Errorf("并发 credit = %v, want %v", s.Total.Credit, float64(goroutines*per)*0.5)
	}
}

func TestResetClears(t *testing.T) {
	c := New()
	c.Record(sample(time.Now(), "u1", "m", 200, 1, 1, 0, false))
	c.Reset()
	if s := c.Snapshot(); s.Total.Requests != 0 {
		t.Fatalf("Reset 后 requests = %d, want 0", s.Total.Requests)
	}
}

func TestDayBoundaryUsesUTC8(t *testing.T) {
	// 直接验证日环索引的切日口径（不依赖 Snapshot 窗口，避免与真实时钟耦合）：
	// UTC 2026-09-25 15:30 == UTC+8 2026-09-25 23:30（同一天）
	// UTC 2026-09-25 16:30 == UTC+8 2026-09-26 00:30（次日）
	c := New()
	d1 := c.days.idxOf(at(2026, time.September, 25, 15))
	d2 := c.days.idxOf(at(2026, time.September, 25, 16))
	if d1 == d2 {
		t.Fatalf("UTC+8 跨零点应切到不同日槽，实际相同 idx=%d", d1)
	}
	// 同一 UTC+8 日内的两个时刻必须同槽：UTC 15:30 与 UTC 16:30 分别是 23:30 / 次日 00:30，
	// 而 UTC 16:30 前一天 UTC+8 15:30（即 UTC 07:30）与 UTC 15:30 同属 9/25。
	if got := c.days.idxOf(at(2026, time.September, 25, 7)); got != d1 {
		t.Errorf("UTC 07:30 (UTC+8 15:30) idx=%d, want 同 %d", got, d1)
	}
}
