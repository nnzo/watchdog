package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	demoinfocs "github.com/markus-wa/demoinfocs-golang/v5/pkg/demoinfocs"
	"github.com/markus-wa/demoinfocs-golang/v5/pkg/demoinfocs/common"
	"github.com/markus-wa/demoinfocs-golang/v5/pkg/demoinfocs/events"
	"github.com/markus-wa/demoinfocs-golang/v5/pkg/demoinfocs/msg"
)

const tickBatchSize = 50_000

// ── Row types ────────────────────────────────────────────────────────────────

type matchRow struct {
	MatchID     string    `ch:"match_id"`
	DemoFile    string    `ch:"demo_file"`
	MapName     string    `ch:"map_name"`
	TickRate    float32   `ch:"tick_rate"`
	TotalTicks  int32     `ch:"total_ticks"`
	TotalRounds int16     `ch:"total_rounds"`
	DurationS   float32   `ch:"duration_s"`
	IngestedAt  time.Time `ch:"ingested_at"`
}

type playerTickRow struct {
	MatchID       string  `ch:"match_id"`
	Tick          int32   `ch:"tick"`
	Round         int16   `ch:"round"`
	SteamID       uint64  `ch:"steam_id"`
	PlayerName    string  `ch:"player_name"`
	Team          int8    `ch:"team"`
	PosX          float32 `ch:"pos_x"`
	PosY          float32 `ch:"pos_y"`
	PosZ          float32 `ch:"pos_z"`
	ViewYaw       float32 `ch:"view_yaw"`
	ViewPitch     float32 `ch:"view_pitch"`
	Health        int16   `ch:"health"`
	Armor         int16   `ch:"armor"`
	IsAlive       uint8   `ch:"is_alive"`
	IsDucking     uint8   `ch:"is_ducking"`
	ActiveWeapon  string  `ch:"active_weapon"`
	Money         int32   `ch:"money"`
	FlashDuration float32 `ch:"flash_duration"`
}

type killEventRow struct {
	MatchID           string  `ch:"match_id"`
	Tick              int32   `ch:"tick"`
	Round             int16   `ch:"round"`
	KillerSteamID     uint64  `ch:"killer_steam_id"`
	KillerName        string  `ch:"killer_name"`
	KillerTeam        int8    `ch:"killer_team"`
	KillerPosX        float32 `ch:"killer_pos_x"`
	KillerPosY        float32 `ch:"killer_pos_y"`
	KillerPosZ        float32 `ch:"killer_pos_z"`
	KillerViewYaw     float32 `ch:"killer_view_yaw"`
	KillerViewPitch   float32 `ch:"killer_view_pitch"`
	VictimSteamID     uint64  `ch:"victim_steam_id"`
	VictimName        string  `ch:"victim_name"`
	VictimTeam        int8    `ch:"victim_team"`
	VictimPosX        float32 `ch:"victim_pos_x"`
	VictimPosY        float32 `ch:"victim_pos_y"`
	VictimPosZ        float32 `ch:"victim_pos_z"`
	Weapon            string  `ch:"weapon"`
	IsHeadshot        uint8   `ch:"is_headshot"`
	PenetratedObjects int32   `ch:"penetrated_objects"`
	ThroughSmoke      uint8   `ch:"through_smoke"`
	AttackerBlind     uint8   `ch:"attacker_blind"`
	NoScope           uint8   `ch:"no_scope"`
	Distance          float32 `ch:"distance"`
}

type playerHurtRow struct {
	MatchID         string  `ch:"match_id"`
	Tick            int32   `ch:"tick"`
	Round           int16   `ch:"round"`
	AttackerSteamID uint64  `ch:"attacker_steam_id"`
	AttackerName    string  `ch:"attacker_name"`
	AttackerTeam    int8    `ch:"attacker_team"`
	AttackerPosX    float32 `ch:"attacker_pos_x"`
	AttackerPosY    float32 `ch:"attacker_pos_y"`
	AttackerPosZ    float32 `ch:"attacker_pos_z"`
	VictimSteamID   uint64  `ch:"victim_steam_id"`
	VictimName      string  `ch:"victim_name"`
	VictimTeam      int8    `ch:"victim_team"`
	VictimPosX      float32 `ch:"victim_pos_x"`
	VictimPosY      float32 `ch:"victim_pos_y"`
	VictimPosZ      float32 `ch:"victim_pos_z"`
	Weapon          string  `ch:"weapon"`
	HealthDamage    int32   `ch:"health_damage"`
	ArmorDamage     int32   `ch:"armor_damage"`
	HitGroup        int32   `ch:"hit_group"`
}

type roundEventRow struct {
	MatchID    string `ch:"match_id"`
	Round      int16  `ch:"round"`
	StartTick  int32  `ch:"start_tick"`
	EndTick    int32  `ch:"end_tick"`
	WinnerTeam int8   `ch:"winner_team"`
	EndReason  int32  `ch:"end_reason"`
	CTScore    int16  `ch:"ct_score"`
	TScore     int16  `ch:"t_score"`
}

// ── Ingester ─────────────────────────────────────────────────────────────────

type ingester struct {
	conn    clickhouse.Conn
	ctx     context.Context
	matchID string
	parser  demoinfocs.Parser

	// Captured from net messages / events during parsing.
	mapName string

	round      int16
	roundStart int32

	tickBuf  []playerTickRow
	killBuf  []killEventRow
	hurtBuf  []playerHurtRow
	roundBuf []roundEventRow

	// pendingRound holds the current open round waiting for a RoundEnd.
	pendingRound *roundEventRow
}

// IngestDemo connects to ClickHouse, ensures the schema exists, parses the
// demo frame-by-frame and batch-inserts all data.
func IngestDemo(cfg *Config, demoPath string) error {
	conn, err := openClickHouse(&cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("connect to clickhouse: %w", err)
	}
	defer conn.Close()

	if err := createSchema(conn, cfg.ClickHouse.Database); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	f, err := os.Open(demoPath)
	if err != nil {
		return fmt.Errorf("open demo: %w", err)
	}
	defer f.Close()

	matchID := strings.TrimSuffix(filepath.Base(demoPath), filepath.Ext(demoPath))
	log.Printf("ingesting %q → match_id=%q", demoPath, matchID)

	ing := &ingester{
		conn:    conn,
		ctx:     context.Background(),
		matchID: matchID,
	}

	p := demoinfocs.NewParser(f)
	defer p.Close()
	ing.parser = p

	// Capture map name from the ServerInfo net-message.
	p.RegisterNetMessageHandler(func(m *msg.CSVCMsg_ServerInfo) {
		ing.mapName = m.GetMapName()
	})

	p.RegisterEventHandler(ing.onFrameDone)
	p.RegisterEventHandler(ing.onKill)
	p.RegisterEventHandler(ing.onPlayerHurt)
	p.RegisterEventHandler(ing.onRoundStart)
	p.RegisterEventHandler(ing.onRoundEnd)

	if err := p.ParseToEnd(); err != nil {
		return fmt.Errorf("parse demo: %w", err)
	}

	// Flush any remaining buffered rows.
	if err := ing.flushAll(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	// Close the last round if it never received a RoundEnd.
	if ing.pendingRound != nil {
		ing.pendingRound.EndTick = int32(p.CurrentFrame())
		ing.roundBuf = append(ing.roundBuf, *ing.pendingRound)
		ing.pendingRound = nil
	}
	if err := ing.flushRounds(); err != nil {
		return fmt.Errorf("flush rounds: %w", err)
	}

	match := matchRow{
		MatchID:     matchID,
		DemoFile:    filepath.Base(demoPath),
		MapName:     ing.mapName,
		TickRate:    float32(p.TickRate()),
		TotalTicks:  int32(p.CurrentFrame()),
		TotalRounds: ing.round,
		DurationS:   float32(p.CurrentTime().Seconds()),
		IngestedAt:  time.Now().UTC(),
	}
	if err := ing.insertMatch(match); err != nil {
		return fmt.Errorf("insert match: %w", err)
	}

	log.Printf("done: map=%q ticks=%d rounds=%d duration=%.0fs",
		ing.mapName, p.CurrentFrame(), ing.round, p.CurrentTime().Seconds())

	return nil
}

// ── Event handlers ────────────────────────────────────────────────────────────

func (ing *ingester) onFrameDone(_ events.FrameDone) {
	gs := ing.parser.GameState()
	tick := int32(ing.parser.CurrentFrame())

	for _, p := range gs.Participants().Playing() {
		if p == nil || p.SteamID64 == 0 {
			continue
		}

		pos := p.Position()
		var weapon string
		if w := p.ActiveWeapon(); w != nil {
			weapon = w.String()
		}

		ing.tickBuf = append(ing.tickBuf, playerTickRow{
			MatchID:       ing.matchID,
			Tick:          tick,
			Round:         ing.round,
			SteamID:       p.SteamID64,
			PlayerName:    p.Name,
			Team:          int8(p.Team),
			PosX:          float32(pos.X),
			PosY:          float32(pos.Y),
			PosZ:          float32(pos.Z),
			ViewYaw:       p.ViewDirectionX(),
			ViewPitch:     p.ViewDirectionY(),
			Health:        int16(p.Health()),
			Armor:         int16(p.Armor()),
			IsAlive:       boolUint8(p.IsAlive()),
			IsDucking:     boolUint8(p.IsDucking()),
			ActiveWeapon:  weapon,
			Money:         int32(p.Money()),
			FlashDuration: p.FlashDuration,
		})
	}

	if len(ing.tickBuf) >= tickBatchSize {
		if err := ing.flushTicks(); err != nil {
			log.Printf("warning: flush player_ticks: %v", err)
		}
	}
}

func (ing *ingester) onKill(e events.Kill) {
	if e.Killer == nil || e.Victim == nil {
		return
	}

	tick := int32(ing.parser.CurrentFrame())
	kPos := e.Killer.Position()
	vPos := e.Victim.Position()

	var weapon string
	if e.Weapon != nil {
		weapon = e.Weapon.String()
	}

	ing.killBuf = append(ing.killBuf, killEventRow{
		MatchID:           ing.matchID,
		Tick:              tick,
		Round:             ing.round,
		KillerSteamID:     e.Killer.SteamID64,
		KillerName:        e.Killer.Name,
		KillerTeam:        int8(e.Killer.Team),
		KillerPosX:        float32(kPos.X),
		KillerPosY:        float32(kPos.Y),
		KillerPosZ:        float32(kPos.Z),
		KillerViewYaw:     e.Killer.ViewDirectionX(),
		KillerViewPitch:   e.Killer.ViewDirectionY(),
		VictimSteamID:     e.Victim.SteamID64,
		VictimName:        e.Victim.Name,
		VictimTeam:        int8(e.Victim.Team),
		VictimPosX:        float32(vPos.X),
		VictimPosY:        float32(vPos.Y),
		VictimPosZ:        float32(vPos.Z),
		Weapon:            weapon,
		IsHeadshot:        boolUint8(e.IsHeadshot),
		PenetratedObjects: int32(e.PenetratedObjects),
		ThroughSmoke:      boolUint8(e.ThroughSmoke),
		AttackerBlind:     boolUint8(e.AttackerBlind),
		NoScope:           boolUint8(e.NoScope),
		Distance:          e.Distance,
	})
}

func (ing *ingester) onPlayerHurt(e events.PlayerHurt) {
	if e.Player == nil {
		return
	}

	tick := int32(ing.parser.CurrentFrame())
	vPos := e.Player.Position()

	var (
		attackerSteamID      uint64
		attackerName         string
		attackerTeam         int8
		aPosX, aPosY, aPosZ float32
	)
	if e.Attacker != nil {
		aPos := e.Attacker.Position()
		attackerSteamID = e.Attacker.SteamID64
		attackerName = e.Attacker.Name
		attackerTeam = int8(e.Attacker.Team)
		aPosX, aPosY, aPosZ = float32(aPos.X), float32(aPos.Y), float32(aPos.Z)
	}

	weapon := e.WeaponString
	if e.Weapon != nil {
		weapon = e.Weapon.String()
	}

	ing.hurtBuf = append(ing.hurtBuf, playerHurtRow{
		MatchID:         ing.matchID,
		Tick:            tick,
		Round:           ing.round,
		AttackerSteamID: attackerSteamID,
		AttackerName:    attackerName,
		AttackerTeam:    attackerTeam,
		AttackerPosX:    aPosX,
		AttackerPosY:    aPosY,
		AttackerPosZ:    aPosZ,
		VictimSteamID:   e.Player.SteamID64,
		VictimName:      e.Player.Name,
		VictimTeam:      int8(e.Player.Team),
		VictimPosX:      float32(vPos.X),
		VictimPosY:      float32(vPos.Y),
		VictimPosZ:      float32(vPos.Z),
		Weapon:          weapon,
		HealthDamage:    int32(e.HealthDamage),
		ArmorDamage:     int32(e.ArmorDamage),
		HitGroup:        int32(e.HitGroup),
	})
}

func (ing *ingester) onRoundStart(_ events.RoundStart) {
	ing.round++
	ing.roundStart = int32(ing.parser.CurrentFrame())
	ing.pendingRound = &roundEventRow{
		MatchID:   ing.matchID,
		Round:     ing.round,
		StartTick: ing.roundStart,
	}
}

func (ing *ingester) onRoundEnd(e events.RoundEnd) {
	if ing.pendingRound == nil {
		return
	}

	ing.pendingRound.EndTick = int32(ing.parser.CurrentFrame())
	ing.pendingRound.WinnerTeam = int8(e.Winner)
	ing.pendingRound.EndReason = int32(e.Reason)

	// TeamState.Score() is not yet updated when RoundEnd fires.
	// Use WinnerState/LoserState from the event and add 1 to the winner's score.
	if e.WinnerState != nil && e.LoserState != nil {
		winScore := int16(e.WinnerState.Score()) + 1
		loseScore := int16(e.LoserState.Score())
		if e.Winner == common.TeamCounterTerrorists {
			ing.pendingRound.CTScore = winScore
			ing.pendingRound.TScore = loseScore
		} else {
			ing.pendingRound.TScore = winScore
			ing.pendingRound.CTScore = loseScore
		}
	}

	ing.roundBuf = append(ing.roundBuf, *ing.pendingRound)
	ing.pendingRound = nil
}

// ── Batch flush helpers ───────────────────────────────────────────────────────

func (ing *ingester) flushTicks() error {
	if len(ing.tickBuf) == 0 {
		return nil
	}
	batch, err := ing.conn.PrepareBatch(ing.ctx, "INSERT INTO player_ticks")
	if err != nil {
		return fmt.Errorf("prepare player_ticks batch: %w", err)
	}
	for i := range ing.tickBuf {
		if err := batch.AppendStruct(&ing.tickBuf[i]); err != nil {
			return fmt.Errorf("append player_ticks row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send player_ticks batch: %w", err)
	}
	log.Printf("  flushed %d player_ticks rows", len(ing.tickBuf))
	ing.tickBuf = ing.tickBuf[:0]
	return nil
}

func (ing *ingester) flushKills() error {
	if len(ing.killBuf) == 0 {
		return nil
	}
	batch, err := ing.conn.PrepareBatch(ing.ctx, "INSERT INTO kill_events")
	if err != nil {
		return fmt.Errorf("prepare kill_events batch: %w", err)
	}
	for i := range ing.killBuf {
		if err := batch.AppendStruct(&ing.killBuf[i]); err != nil {
			return fmt.Errorf("append kill_events row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send kill_events batch: %w", err)
	}
	log.Printf("  flushed %d kill_events rows", len(ing.killBuf))
	ing.killBuf = ing.killBuf[:0]
	return nil
}

func (ing *ingester) flushHurts() error {
	if len(ing.hurtBuf) == 0 {
		return nil
	}
	batch, err := ing.conn.PrepareBatch(ing.ctx, "INSERT INTO player_hurt")
	if err != nil {
		return fmt.Errorf("prepare player_hurt batch: %w", err)
	}
	for i := range ing.hurtBuf {
		if err := batch.AppendStruct(&ing.hurtBuf[i]); err != nil {
			return fmt.Errorf("append player_hurt row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send player_hurt batch: %w", err)
	}
	log.Printf("  flushed %d player_hurt rows", len(ing.hurtBuf))
	ing.hurtBuf = ing.hurtBuf[:0]
	return nil
}

func (ing *ingester) flushRounds() error {
	if len(ing.roundBuf) == 0 {
		return nil
	}
	batch, err := ing.conn.PrepareBatch(ing.ctx, "INSERT INTO round_events")
	if err != nil {
		return fmt.Errorf("prepare round_events batch: %w", err)
	}
	for i := range ing.roundBuf {
		if err := batch.AppendStruct(&ing.roundBuf[i]); err != nil {
			return fmt.Errorf("append round_events row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send round_events batch: %w", err)
	}
	log.Printf("  flushed %d round_events rows", len(ing.roundBuf))
	ing.roundBuf = ing.roundBuf[:0]
	return nil
}

func (ing *ingester) flushAll() error {
	if err := ing.flushTicks(); err != nil {
		return err
	}
	if err := ing.flushKills(); err != nil {
		return err
	}
	return ing.flushHurts()
}

func (ing *ingester) insertMatch(m matchRow) error {
	batch, err := ing.conn.PrepareBatch(ing.ctx, "INSERT INTO matches")
	if err != nil {
		return fmt.Errorf("prepare matches batch: %w", err)
	}
	if err := batch.AppendStruct(&m); err != nil {
		return fmt.Errorf("append match row: %w", err)
	}
	return batch.Send()
}

// ── Utilities ─────────────────────────────────────────────────────────────────

func boolUint8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}
