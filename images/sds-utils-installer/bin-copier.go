/*
Copyright 2024 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/


package main

import (
    "fmt"
    "os"
    "io"
    "io/ioutil"
    "path/filepath"
    "sort"
    "strings"
    "log"
    "crypto/sha256"
    "encoding/hex"
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


func CopyDir(src, dst string) error {
    return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }

        dstPath := filepath.Join(dst, strings.TrimPrefix(path, src))
        if info.IsDir() {
            return os.MkdirAll(dstPath, info.Mode())
        }

        srcFile, err := os.Open(path)
        if err != nil {
            return err
        }
        defer srcFile.Close()

        dstFile, err := os.Create(dstPath)
        if err != nil {
            return err
        }
        
        _, err = io.Copy(dstFile, srcFile)
        dstFile.Close()

        return err
    })
}




func main() {
	source := os.Args[1]
	dest := os.Args[2]


	info_src, err := os.Stat(source)
	if os.IsNotExist(err) {
		log.Fatal("ERR: source path doesn't exist!")
	} else if !info_src.IsDir() {
		log.Fatal("ERR: source path is a file (expecting dir)!")
	}

        info_dst, err := os.Stat(dest)
        if os.IsNotExist(err) {
                log.Fatal("ERR: destination path doesn't exist!")
        } else if !info_dst.IsDir() {
                log.Fatal("ERR: destination path is a file (expecting dir)!")
        }


        srcHash, err := DirHash(source)
    	if err != nil {
        	log.Fatal("ERR failed calculating src dir hash")
	}

	dstHash, err := DirHash(dest)
	if err != nil {
        	log.Fatal("ERR failed calculating destination dir hash")
	}

	if srcHash != dstHash {
        	err = CopyDir(source, dest)
        	if err != nil {
            		log.Fatal("ERR failed copying files")
	        }
        		fmt.Println("Directories were different. Copied files from source to destination.")
		} else {
        		fmt.Println("Directories are identical. No files were copied.")
	 }



}
