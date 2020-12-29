package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

const (
	MB = 1 << 20

	chunkSize = 8 * MB
)

func main() {
	log.SetFlags(log.Lshortfile | log.Ltime | log.Lmicroseconds | log.LUTC)
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	args := os.Args[1:]
	if len(args) < 1 {
		return errors.New("missing src file")
	}
	if len(args) < 2 {
		return errors.New("missing dst file")
	}
	inFile := args[0]
	outFile := args[1]

	if err := doCopy(inFile, outFile); err != nil {
		return err
	}

	return nil
}

func doCopy(inFile, outFile string) error {
	in, err := os.Open(inFile)
	if err != nil {
		return err
	}
	defer in.Close()

	fi, err := in.Stat()
	if err != nil {
		return err
	}

	inSize := fi.Size()

	if inSize < chunkSize*2 {
		out, err := os.Create(outFile)
		if err != nil {
			return err
		}
		defer out.Close()
		if _, err := io.Copy(out, in); err != nil {
			return err
		}
		return nil
	}

	if err := createOutFile(outFile, inSize); err != nil {
		return err
	}

	rwinfos, err := getRWInfos(inSize, in, outFile)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(len(rwinfos))

	for i, rwi := range rwinfos {
		go func(wg *sync.WaitGroup, info rwinfo, i int) {
			defer wg.Done()
			n, err := io.Copy(info.w, info.r)
			if err != nil {
				log.Println(n, err)
				return
			}
		}(&wg, rwi, i)
	}

	wg.Wait()

	for _, rwi := range rwinfos {
		rwi.w.Close()
	}

	return nil
}

func createOutFile(outFile string, inSize int64) error {
	out, err := os.Create(outFile)
	if err != nil {
		return err
	}
	if err := out.Truncate(inSize); err != nil {
		return err
	}
	out.Close()

	return nil
}

func getRWInfos(inSize int64, in *os.File, outFile string) ([]rwinfo, error) {

	n := int(inSize / chunkSize)

	rwinfos := make([]rwinfo, 0, n)
	for i := 0; i < n-1; i++ {
		r := io.NewSectionReader(in, int64(i*chunkSize), chunkSize)
		w, err := NewSectionWriter(outFile, int64(i*chunkSize), chunkSize)
		if err != nil {
			return nil, err
		}
		rwinfos = append(rwinfos, rwinfo{r, w})
	}

	// add last element
	off := int64((n - 1) * chunkSize)
	size := inSize - off
	r := io.NewSectionReader(in, off, size)
	w, err := NewSectionWriter(outFile, off, size)
	if err != nil {
		return nil, err
	}
	rwinfos = append(rwinfos, rwinfo{r, w})

	return rwinfos, nil
}

type rwinfo struct {
	r *io.SectionReader
	w *SectionWriter
}

type SectionWriter struct {
	w     *os.File
	base  int64
	off   int64
	limit int64
}

func NewSectionWriter(path string, off int64, n int64) (*SectionWriter, error) {
	w, err := os.OpenFile(path, os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	if _, err := w.Seek(off, 0); err != nil {
		return nil, err
	}
	return &SectionWriter{w, off, off, off + n}, nil
}

func (s SectionWriter) Close() {
	s.w.Close()
}

func (s *SectionWriter) Write(b []byte) (n int, err error) {
	if s.off >= s.limit {
		return 0, io.EOF
	}
	if max := s.limit - s.off; int64(len(b)) > max {
		return 0, io.ErrShortWrite
	}
	n, err = s.w.Write(b)
	s.off += int64(n)
	return
}
