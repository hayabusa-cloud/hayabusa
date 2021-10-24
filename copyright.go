package hayabusa

import (
	"io"
	"os"
)

var hayabusaLogo = `
┏┓╋╋╋╋╋╋╋╋╋╋╋┏┓
┃┃╋╋╋╋╋╋╋╋╋╋╋┃┃
┃┗━┳━━┳┓╋┏┳━━┫┗━┳┓┏┳━━┳━━┓
┃┏┓┃┏┓┃┃╋┃┃┏┓┃┏┓┃┃┃┃━━┫┏┓┃
┃┃┃┃┏┓┃┗━┛┃┏┓┃┗┛┃┗┛┣━━┃┏┓┃
┗┛┗┻┛┗┻━┓┏┻┛┗┻━━┻━━┻━━┻┛┗┛
╋╋╋╋╋╋┏━┛┃
╋╋╋╋╋╋┗━━┛        🅲🅻🅾🆄🅳
`
var copyrightInformation = "© 2021 HAYABUSA Cloud\nE-mail: git@hybscloud.com\n"

func printLogo() {
	io.WriteString(os.Stdout, hayabusaLogo)
	io.WriteString(os.Stdout, copyrightInformation)
}
