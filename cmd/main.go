package main

import (
	"bufio"
	"context"
	"log"
	"os"
	"strings"
	"sync"
)

type result struct {
	rowsCount         int
	peopleCount       int
	commonName        string
	commonNameCount   int
	donationMonthFreq map[string]int
}

func main() {
	concurrent("input.csv", 10, 1000)
}

func processRow(text string) (firstName, fullName, month string) {
	row := strings.Split(text, "|")

	fullName = strings.Replace(strings.TrimSpace(row[7]), " ", "", -1)
	name := strings.TrimSpace(row[7])
	if name != "" {
		startOfName := strings.Index(name, ", ") + 2
		if endOfName := strings.Index(name[startOfName:], " "); endOfName < 0 {
			firstName = name[startOfName:]
		} else {
			firstName = name[startOfName : startOfName+endOfName]
		}
		if strings.HasSuffix(firstName, ",") {
			firstName = strings.Replace(firstName, ",", "", -1)
		}
	}

	date := strings.TrimSpace(row[13])
	if len(date) == 8 {
		month = date[:2]
	} else {
		month = "--"
	}

	return firstName, fullName, month
}

func concurrent(file string, numOfWorkers, batchSize int) (res result) {
	res = result{donationMonthFreq: map[string]int{}}

	type processed struct {
		numRows    int
		fullNames  []string
		firstNames []string
		months     []string
	}

	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}

	// reader creates and returns a channel that receives
	// batches of rows (of length batchSize) from the file
	reader := func(ctx context.Context, rowsBatch *[]string) <-chan []string {
		outChan := make(chan []string)
		scanner := bufio.NewScanner(f)

		go func() {
			defer close(outChan) // close channel when we are done sending all rows

			for {
				scanned := scanner.Scan()

				select {
				case <-ctx.Done():
					return
				default:
					row := scanner.Text()
					// if batch size is complete or end of file, send batch out
					if len(*rowsBatch) == batchSize || !scanned {
						outChan <- *rowsBatch
						*rowsBatch = []string{} // clear batch
					}
					*rowsBatch = append(*rowsBatch, row) // add row to current batch
				}

				// if nothing else to scan return
				if !scanned {
					return
				}
			}
		}()
		return outChan
	}

	// worker takes in a read-only channel to receive batches of rows.
	// After it processes each row-batch it sends out the processed output
	// on its channel.
	worker := func(ctx context.Context, inChan <-chan []string) <-chan processed {
		outChan := make(chan processed)

		go func() {
			defer close(outChan)

			p := processed{}
			for rowsBatch := range inChan {
				for _, row := range rowsBatch {
					firstName, fullName, month := processRow(row)
					p.fullNames = append(p.fullNames, fullName)
					p.firstNames = append(p.firstNames, firstName)
					p.months = append(p.months, month)
					p.numRows++
				}
			}
			outChan <- p
		}()
		return outChan
	}

	// combiner takes in multiple read-only channels that receive processed output
	// (from workers) and sends it out on its own channel via a multiplexer.
	combiner := func(ctx context.Context, inputChannels ...<-chan processed) <-chan processed {
		outChan := make(chan processed)

		var wg sync.WaitGroup
		multiplexer := func(in <-chan processed) {
			defer wg.Done()

			for processedData := range in {
				select {
				case <-ctx.Done():
				case outChan <- processedData:
				}
			}
		}

		// add length of input channels to be consumed by mutiplexer
		wg.Add(len(inputChannels))
		for _, ch := range inputChannels {
			go multiplexer(ch)
		}

		// close channel after all inputs channels are closed
		go func() {
			wg.Wait()
			close(outChan)
		}()

		return outChan
	}

	// create a main context, and call cancel at the end, to ensure all our
	// goroutines exit without leaving leaks.
	// Particularly, if this function becomes part of a program with
	// a longer lifetime than this function.
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	// STAGE 1: start reader
	var rowsBatch []string
	rowsChan := reader(ctx, &rowsBatch)

	// STAGE 2: create a slice of processed output channels with size of numWorkers
	// and assign each slot with the out channel from each worker.
	processedDataChannels := make([]<-chan processed, numOfWorkers)
	for i := 0; i < numOfWorkers; i++ {
		processedDataChannels[i] = worker(ctx, rowsChan)
	}

	firstNameCount := map[string]int{}
	fullNameCount := map[string]bool{}

	// STAGE 3: read from the combined channel and calculate the final result.
	// this will end once all channels from workers are closed!
	for processedData := range combiner(ctx, processedDataChannels...) {
		// add number of rows processed by worker
		res.rowsCount += processedData.numRows

		// add months processed by worker
		for _, month := range processedData.months {
			res.donationMonthFreq[month]++
		}

		// use full-names to count people
		for _, fullName := range processedData.fullNames {
			fullNameCount[fullName] = true
		}
		res.peopleCount = len(fullNameCount)

		// update most common first name based on processed results
		for _, firstName := range processedData.firstNames {
			firstNameCount[firstName]++

			if firstNameCount[firstName] > res.commonNameCount {
				res.commonName = firstName
				res.commonNameCount = firstNameCount[firstName]
			}
		}
	}

	return res
}
