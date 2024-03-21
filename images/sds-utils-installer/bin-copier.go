package main

import (
    "io"
    "os"
    "path/filepath"
    "crypto/sha256"
    "encoding/hex"
    "io/ioutil"
    "log"
    "fmt"
    "sort"
    "strings"

)


func DirHash(path string) (string, error) {
    var hashes []string

    err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }
        if info.IsDir() {
            return nil
        }

        data, err := ioutil.ReadFile(path)
        if err != nil {
            return err
        }

        hasher := sha256.New()
        hasher.Write(data)

        hashes = append(hashes, hex.EncodeToString(hasher.Sum(nil)))

        return nil
    })
    if err != nil {
        return "", err
    }

    sort.Strings(hashes)
    return hex.EncodeToString(sha256.New().Sum([]byte(strings.Join(hashes, "")))), nil
 }



func FileHash(path string) (string, error) {
    data, err := ioutil.ReadFile(path)
    if err != nil {
        return "", err
    }

    hasher := sha256.New()
    hasher.Write(data)

    return hex.EncodeToString(hasher.Sum(nil)), nil
}


func CopyIfDifferent(src, dst string) error {
    return filepath.Walk(src, func(srcPath string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }

        dstPath := filepath.Join(dst, strings.TrimPrefix(srcPath, src))
        if info.IsDir() {
            return os.MkdirAll(dstPath, info.Mode())
        }

        srcHash, err := FileHash(srcPath)
        if err != nil {
            return err
        }

        dstHash, _ := FileHash(dstPath) 

        if srcHash != dstHash {
            srcFile, err := os.Open(srcPath)
            if err != nil {
                return err
            }
            defer srcFile.Close()

            dstFile, err := os.Create(dstPath)
            if err != nil {
                return err
            }
	    fmt.Println(dstPath)
            defer dstFile.Close()

            _, err = io.Copy(dstFile, srcFile)
            return err
        }

        return nil
    })
}


func main() {
    src := os.Args[1]
    dst := os.Args[2]


        info_src, err := os.Stat(src)
        if os.IsNotExist(err) {
                log.Fatal("ERR: source path doesn't exist!")
        } else if !info_src.IsDir() {
                log.Fatal("ERR: source path is a file (expecting dir)!")
        }

        info_dst, err := os.Stat(dst)
        if os.IsNotExist(err) {
                log.Fatal("ERR: destination path doesn't exist!")
        } else if !info_dst.IsDir() {
                log.Fatal("ERR: destination path is a file (expecting dir)!")
        }


        srcHash, err := DirHash(src)
        if err != nil {
                log.Fatal("ERR failed calculating src dir hash")
        }

        dstHash, err := DirHash(dst)
        if err != nil {
        log.Fatal("ERR failed calculating destination dir hash")
        }

        if srcHash != dstHash {
                err := CopyIfDifferent(src, dst)
                if err != nil {
                        log.Fatal("ERR failed copying files")
                }
                        fmt.Println("Directories' sha256 sum is different, see what's been copied above")
                } else {
                        fmt.Println("Directories are identical. No files were copied.")
         }


}

	
