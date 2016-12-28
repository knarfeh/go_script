package main

import "time"

func main() {
	exit := make(chan struct{})

	go func() {
		time.Sleep(time.Second)
		println("goroutine done.")

		close(exit)
	}()

	println("main ...")
	<-exit
	println("main exit")
}
