package util

import (
	"io/ioutil"
	"os"
	"strings"
)

const defaultPermDir = 0777

func MakeDirIfNotExists(dir string) error {
	if err := os.Mkdir(dir, defaultPermDir); err != nil && !os.IsExist(err) {
		return err
	}
	return nil
}

func listDirFiles(dirPath string, fileExt string) ([]string, error) {
	fileInfos, err := ioutil.ReadDir(dirPath)

	if err != nil {
		return nil, err
	}

	files := make([]string, 0)
	for _, fileInfo := range fileInfos {
		if strings.HasSuffix(fileInfo.Name(), fileExt) {
			files = append(files, dirPath+"/"+fileInfo.Name())
		}
	}
	return files, nil
}

func ListDir(dir string, ext string) ([]string, error) {
	fileNames, err := listDirFiles(dir, ext)
	if err != nil {
		return nil, err
	}
	return fileNames, nil
}
