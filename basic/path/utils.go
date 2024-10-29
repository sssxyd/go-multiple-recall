package path

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

func GetExecutionPath() string {
	exePath, err := os.Executable()
	if err != nil {
		fmt.Println("Error getting executable path:", err)
		return ""
	}
	exeDir := filepath.Dir(exePath)

	// 判断是否在临时目录中运行（典型的 go run 行为）
	if strings.Contains(exePath, os.TempDir()) {
		_, filename, _, ok := runtime.Caller(0)
		if !ok {
			fmt.Println("Failed to get caller information")
			return ""
		}
		srcDir := filepath.Dir(filename)
		return filepath.Dir(filepath.Dir(srcDir))
	} else {
		// 默认返回可执行文件所在目录
		return exeDir
	}
}

func IsPathExist(path string) bool {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	} else if err != nil {
		log.Printf("An error occurred while checking the path %s: %v\n", path, err)
		return false
	} else {
		return true
	}
}

func TouchDir(path string) error {
	// 检查目录是否存在
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// 创建目录
		err := os.MkdirAll(path, 0755) // 0755 权限设置允许所有者读写执行，组和其他用户只读执行
		if err != nil {
			return err
		}
	}
	return nil
}

func OpenLogFile(logFilePath string) (*os.File, error) {
	logFilDir := filepath.Dir(logFilePath)
	TouchDir(logFilDir)
	// 确保目录存在
	if err := os.MkdirAll(logFilDir, 0755); err != nil {
		print("Failed to create directory: %v", err)
		return nil, err
	}
	// 打开或创建日志文件
	return os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
}

/*
InitializeLogFile initializes the log file and returns the file pointer.
Parameters:

	logFilePath: the path of the log file.
	stdOut: whether to output to stdout.

Returns:

	*os.File: the file pointer of the log file.
*/
func InitializeLogFile(logFilePath string, stdOut bool) (*os.File, error) {
	// 设置日志前缀包含长文件名和行号
	log.SetFlags(log.Lshortfile | log.Ldate | log.Ltime | log.Lmicroseconds)

	// 打开或创建日志文件
	logFile, err := OpenLogFile(logFilePath)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
		return nil, err
	}

	if stdOut {
		//设置 MultiWriter，同时输出到文件和 stdout
		mw := io.MultiWriter(os.Stdout, logFile)
		log.SetOutput(mw)
	} else {
		log.SetOutput(logFile)
	}
	return logFile, err
}
