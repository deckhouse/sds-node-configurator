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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func contentsHash(path string) (string, error) {
	var hashes []string

	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		data, err := os.ReadFile(path)
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

func copyIfDifferent(src, dst string) error {
	return filepath.Walk(src, func(srcPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		dstPath := filepath.Join(dst, strings.TrimPrefix(srcPath, src))
		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		srcHash, err := contentsHash(srcPath)
		if err != nil {
			return err
		}

		dstHash, _ := contentsHash(dstPath)

		if srcHash != dstHash {
			srcFile, err := os.Open(srcPath)
			if err != nil {
				fmt.Println("Error getting file from source")
			}
			defer srcFile.Close()

			dstFile, err := os.Create(dstPath)
			if err != nil {
				fmt.Println("Error copying file to destination folder")
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

	srcCheck, err := os.Stat(src)
	if os.IsNotExist(err) {
		log.Fatal("ERR: source path doesn't exist!")
	} else if !srcCheck.IsDir() {
		log.Fatal("ERR: source path is a file (expecting dir)!")
	}

	dstCheck, err := os.Stat(dst)
	if os.IsNotExist(err) {
		log.Fatal("ERR: destination path doesn't exist!")
	} else if !dstCheck.IsDir() {
		log.Fatal("ERR: destination path is a file (expecting dir)!")
	}

	srcHash, err := contentsHash(src)
	if err != nil {
		log.Fatal("ERR failed calculating src dir hash")
	}

	dstHash, err := contentsHash(dst)
	if err != nil {
		log.Fatal("ERR failed calculating destination dir hash")
	}

	if srcHash != dstHash {
		fmt.Println("Found new files, copying:")
		err := copyIfDifferent(src, dst)
		if err != nil {
			log.Fatal("ERR failed copying files")
		}
		fmt.Println("Utils have been updated successfully.")
	} else {
		fmt.Println("Everything is up-to-date. No files were copied.")
	}

}
