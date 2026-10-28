package main
import("bufio";"fmt";"os";"strconv";"strings")
func main(){s:=bufio.NewScanner(os.Stdin);w:=bufio.NewWriter(os.Stdout);defer w.Flush();for s.Scan(){f:=strings.Fields(s.Text());seed,e:=strconv.ParseUint(f[0],10,64);if e!=nil{panic(e)};id,e:=strconv.ParseUint(f[1],10,64);if e!=nil{panic(e)};n,e:=strconv.Atoi(f[2]);if e!=nil{panic(e)};fmt.Fprintf(w,"%x %x\n",MakeKey(id,seed),MakeValue(id,seed,n))};if e:=s.Err();e!=nil{panic(e)}}
