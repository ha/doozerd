package main

import (
	"doozer/client"
	"log"
)


func claim(name, baddr, laddr string) (aaddr string) {
	c := client.New("", baddr)
	_, err := c.Set("/ctl/boot/"+name, 0, []byte(laddr))
	switch err {
	case client.ErrOldRev:
		log.Println("not us, find out who")
		v, _, err := c.Get("/ctl/boot/"+name, nil)
		if err != nil {
			panic(err)
		}
		log.Println("it is", string(v))
		return string(v)
	case nil:
		log.Println("we are seed")
		return "" // we are the seed node -- don't attach
	}
	panic(err)
}
