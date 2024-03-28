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
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
)

func main() {
	srcDir := os.Args[1]
	dstDir := os.Args[2]

	srcCheck, err := os.Stat(srcDir)
	if os.IsNotExist(err) {
		log.Fatal("ERR: source path doesn't exist!")
	} else if !srcCheck.IsDir() {
		log.Fatal("ERR: source path is a file (expecting dir)!")
	}

	dstCheck, err := os.Stat(dstDir)
	if os.IsNotExist(err) {
		log.Fatal("ERR: destination path doesn't exist!")
	} else if !dstCheck.IsDir() {
		log.Fatal("ERR: destination path is a file (expecting dir)!")
	}

	err = copyFilesRecursive(srcDir, dstDir)
	if err != nil {
		log.Println("Error:", err)
		return
	}

	log.Println("Done.")
}

func copyFile(src, dst string) (err error) {
	var srcFile, dstFile *os.File
	srcFile, err = os.Open(src)
	if err != nil {
		return err
	}
	defer func(srcFile *os.File) {
		errClose := srcFile.Close()
		if errClose != nil {
			err = errors.Join(err, errClose)
		}
	}(srcFile)

	dstFile, err = os.Create(dst)
	if err != nil {
		return err
	}
	defer func(dstFile *os.File) {
		errClose := dstFile.Close()
		if errClose != nil {
			err = errors.Join(err, errClose)
		}
	}(dstFile)

	_, err = io.Copy(dstFile, srcFile)
	return err
}

func getChecksum(filePath string) (checksum string, err error) {
	var file *os.File
	file, err = os.Open(filePath)
	if err != nil {
		return
	}
	defer func(file *os.File) {
		errClose := file.Close()
		if errClose != nil {
			err = errors.Join(err, errClose)
		}
	}(file)

	hash := sha256.New()
	if _, err = io.Copy(hash, file); err != nil {
		return
	}

	checksum = hex.EncodeToString(hash.Sum(nil))
	return
}

func copyFilesRecursive(srcDir, dstDir string) error {
	err := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}

		dstPath := filepath.Join(dstDir, relPath)

		if info.IsDir() {
			log.Println("Checking subfolder", dstPath)
			return os.MkdirAll(dstPath, info.Mode())
		}

		if _, err := os.Stat(dstPath); err == nil {
			srcChecksum, err := getChecksum(path)
			if err != nil {
				return err
			}
			log.Println(dstPath, "- File already exists, checking sha256 sum..")
			dstChecksum, err := getChecksum(dstPath)
			if err != nil {
				return err
			}

			if srcChecksum == dstChecksum {
				log.Printf("Skipping %s: Checksum is the same\n", path)
				return nil
			} else {
				log.Println("Copying\n", path)
			}
		}

		err = copyFile(path, dstPath)
		if err != nil {
			return err
		}

		log.Printf("Copied %s successfully\n", path)

		return nil
	})

	return err
}
