package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func openClickHouse(cfg *ClickHouseConfig) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout:     30 * time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    5,
		ConnMaxLifetime: 10 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	return conn, nil
}

// schemaSQL contains all CREATE TABLE statements separated by semicolons.
const schemaSQL = `
CREATE TABLE IF NOT EXISTS matches (
    match_id     String,
    demo_file    String,
    map_name     String,
    tick_rate    Float32,
    total_ticks  Int32,
    total_rounds Int16,
    duration_s   Float32,
    ingested_at  DateTime
) ENGINE = MergeTree()
ORDER BY match_id;

CREATE TABLE IF NOT EXISTS player_ticks (
    match_id      String,
    tick          Int32,
    round         Int16,
    steam_id      UInt64,
    player_name   String,
    team          Int8,
    pos_x         Float32,
    pos_y         Float32,
    pos_z         Float32,
    view_yaw      Float32,
    view_pitch    Float32,
    health        Int16,
    armor         Int16,
    is_alive      UInt8,
    is_ducking    UInt8,
    active_weapon String,
    money         Int32,
    flash_duration Float32
) ENGINE = MergeTree()
ORDER BY (match_id, steam_id, tick);

CREATE TABLE IF NOT EXISTS kill_events (
    match_id           String,
    tick               Int32,
    round              Int16,
    killer_steam_id    UInt64,
    killer_name        String,
    killer_team        Int8,
    killer_pos_x       Float32,
    killer_pos_y       Float32,
    killer_pos_z       Float32,
    killer_view_yaw    Float32,
    killer_view_pitch  Float32,
    victim_steam_id    UInt64,
    victim_name        String,
    victim_team        Int8,
    victim_pos_x       Float32,
    victim_pos_y       Float32,
    victim_pos_z       Float32,
    weapon             String,
    is_headshot        UInt8,
    penetrated_objects Int32,
    through_smoke      UInt8,
    attacker_blind     UInt8,
    no_scope           UInt8,
    distance           Float32
) ENGINE = MergeTree()
ORDER BY (match_id, tick);

CREATE TABLE IF NOT EXISTS player_hurt (
    match_id          String,
    tick              Int32,
    round             Int16,
    attacker_steam_id UInt64,
    attacker_name     String,
    attacker_team     Int8,
    attacker_pos_x    Float32,
    attacker_pos_y    Float32,
    attacker_pos_z    Float32,
    victim_steam_id   UInt64,
    victim_name       String,
    victim_team       Int8,
    victim_pos_x      Float32,
    victim_pos_y      Float32,
    victim_pos_z      Float32,
    weapon            String,
    health_damage     Int32,
    armor_damage      Int32,
    hit_group         Int32
) ENGINE = MergeTree()
ORDER BY (match_id, tick);

CREATE TABLE IF NOT EXISTS round_events (
    match_id    String,
    round       Int16,
    start_tick  Int32,
    end_tick    Int32,
    winner_team Int8,
    end_reason  Int32,
    ct_score    Int16,
    t_score     Int16
) ENGINE = MergeTree()
ORDER BY (match_id, round)`

func createSchema(conn clickhouse.Conn, database string) error {
	ctx := context.Background()

	if err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database)); err != nil {
		return fmt.Errorf("create database: %w", err)
	}

	for _, stmt := range splitSQL(schemaSQL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("execute schema statement: %w\nSQL: %s", err, stmt)
		}
	}

	return nil
}

func splitSQL(sql string) []string {
	var stmts []string
	for _, s := range strings.Split(sql, ";") {
		s = strings.TrimSpace(s)
		if s != "" {
			stmts = append(stmts, s)
		}
	}
	return stmts
}
