package main

import (
	"log"

	demoinfocs "github.com/markus-wa/demoinfocs-golang/v5/pkg/demoinfocs"
	events "github.com/markus-wa/demoinfocs-golang/v5/pkg/demoinfocs/events"
)

func onKill(kill events.Kill) {
	var hs string
	if kill.IsHeadshot {
		hs = " (HS)"
	}

	var wallBang string
	if kill.PenetratedObjects > 0 {
		wallBang = " (WB)"
	}

	log.Printf("%s <%v%s%s> %s\n", kill.Killer, kill.Weapon, hs, wallBang, kill.Victim)
}

func main() {
	err := demoinfocs.ParseFile("demos/match730_003806635570348687519_0338214601_423.dem", func(p demoinfocs.Parser) error {
		p.RegisterEventHandler(onKill)

		return nil
	})
	if err != nil {
		log.Panic("failed to parse demo: ", err)
	}
}