package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

const (
	chunkSize = 8 << 20
)

func main() {
	log.SetFlags(log.Lshortfile | log.Ltime | log.Lmicroseconds | log.LUTC)
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func conc(inFile, outFile string) error {
	in, err := os.Open(inFile)
	if err != nil {
		return err
	}
	defer in.Close()

	fi, err := in.Stat()
	if err != nil {
		return err
	}

	fileSize := fi.Size()

	out, err := os.Create(outFile)
	if err != nil {
		return err
	}
	if err := out.Truncate(fileSize); err != nil {
		return err
	}
	out.Close()

	n := int(fileSize / chunkSize)

	rwinfos := make([]rwinfo, 0, n)
	for i := 0; i < n; i++ {
		r := io.NewSectionReader(in, int64(i*chunkSize), chunkSize)
		w, err := NewSectionWriter(outFile, int64(i*chunkSize), chunkSize)
		if err != nil {
			return err
		}
		defer w.Close()
		rwinfos = append(rwinfos, rwinfo{r, w})
	}

	var wg sync.WaitGroup
	wg.Add(n)

	for i, rwi := range rwinfos {
		go func(wg *sync.WaitGroup, info rwinfo, i int) {
			defer wg.Done()
			buf := make([]byte, chunkSize)
			n, err := io.CopyBuffer(info.w, info.r, buf)
			if err != nil {
				log.Println(n, err)
				return
			}
			if n != chunkSize {
				log.Println(i, n)
			}
		}(&wg, rwi, i)
	}

	wg.Wait()

	return nil
}

func seq(inFile, outFile string) error {
	in, err := os.Open(inFile)
	if err != nil {
		return err
	}
	defer in.Close()

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

func run() error {
	var mode string
	flag.StringVar(&mode, "m", "s", "Mode: sequential(s) or concurrent(c)")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		return errors.New("missing src file")
	}
	if len(args) < 2 {
		return errors.New("missing dst file")
	}
	inFile := args[0]
	outFile := args[1]

	if mode == "c" {
		if err := conc(inFile, outFile); err != nil {
			return err
		}
	} else {
		if err := seq(inFile, outFile); err != nil {
			return err
		}
	}

	return nil
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
