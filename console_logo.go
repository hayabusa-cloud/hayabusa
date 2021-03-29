package hybs

import (
	"io"
	"os"
)

var serverLogo = `
┏┓╋╋╋╋╋╋╋╋╋╋╋┏┓
┃┃╋╋╋╋╋╋╋╋╋╋╋┃┃
┃┗━┳━━┳┓╋┏┳━━┫┗━┳┓┏┳━━┳━━┓
┃┏┓┃┏┓┃┃╋┃┃┏┓┃┏┓┃┃┃┃━━┫┏┓┃
┃┃┃┃┏┓┃┗━┛┃┏┓┃┗┛┃┗┛┣━━┃┏┓┃
┗┛┗┻┛┗┻━┓┏┻┛┗┻━━┻━━┻━━┻┛┗┛
╋╋╋╋╋╋┏━┛┃
╋╋╋╋╋╋┗━━┛
`
var authorInfo = "© 2021 HAYABUSA Cloud\nEメール: hayabusa-cloud@outlook.jp\n"

func printLogo() {
	io.WriteString(os.Stdout, serverLogo)
	io.WriteString(os.Stdout, authorInfo)
}
