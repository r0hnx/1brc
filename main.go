package main

import (
	m "1brc/models"
	u "1brc/utils"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
)

const DATA = "measurements.txt"
const BUFFER_SIZE = 100
const CHUNK_SIZE = 64 * 1024 * 1024

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func main() {
	// Open the file for reading
	// This line opens the file specified by the constant DATA for reading.
	// If the file cannot be opened, an error is returned and printed to the console.
	// The defer statement ensures that the file is closed at the end of the function, even if an error occurs.
	file, err := os.Open(DATA)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	// Evaluate the data in the file
	// The Evaluate function is called with the opened file as an argument.
	// This function likely processes the data in the file and returns a slice of cities, along with a possible error.
	cities, err := Evaluate(file)
	if err != nil {
		// If an error occurs during the evaluation, the program will panic, which will terminate the program and print the error message.
		panic(err)
	}

	// Write the weather statistics
	// The WriteWeatherStats function is called with the slice of cities as an argument.
	// This function likely writes the weather statistics for each city to some output, such as a file or the console.
	u.WriteWeatherStats(cities)

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}

func Evaluate(file *os.File) (map[string]m.Weather, error) {
	// This function reads and processes the weather data from the given file.
	// It returns a map of cities and their corresponding weather statistics, or an error if something goes wrong.

	cities := make(map[string]m.Weather)                // Declare a map to store the final weather statistics for each city
	citiesStream := make(chan map[string]m.Weather, 10) // Declare a channel to receive the processed chunks of weather data
	chunkStream := make(chan []byte, 15)                // Declare a channel to send the input data chunks to be processed
	var wg sync.WaitGroup                               // Declare a wait group to ensure all goroutines have finished before returning

	// Start worker goroutines to process the input data chunks
	for i := 0; i < runtime.NumCPU()-1; i++ {
		wg.Add(1)
		go func() {
			for chunk := range chunkStream {
				// For each chunk received on the chunkStream channel, call the ChunkReader function
				// to process the chunk and send the results to the citiesStream channel
				ChunkReader(chunk, citiesStream)
			}
			wg.Done()
		}()
	}

	// Start a goroutine to read the input file and send the data to the chunkStream channel
	go func() {
		selected := make([]byte, CHUNK_SIZE)    // Declare a buffer to store the read data
		leftover := make([]byte, 0, CHUNK_SIZE) // Declare a buffer to store any leftover data from the previous chunk

		for {
			// Read a chunk of data from the input file
			read, err := file.Read(selected)
			if err != nil {
				if errors.Is(err, io.EOF) {
					// If we've reached the end of the file, break out of the loop
					break
				}
				panic(err) // If any other error occurs, panic
			}

			selected = selected[:read] // Adjust the selected slice to the actual read size
			res := make([]byte, read)  // Create a new slice to hold the read data
			copy(res, selected)        // Copy the read data to the new slice

			// Find the last newline character in the read data
			lastNewLineIndex := bytes.LastIndex(selected, []byte{'\n'})
			// Append any leftover data from the previous chunk to the current chunk
			res = append(leftover, selected[:lastNewLineIndex+1]...)
			// Update the leftover data buffer with any remaining data after the last newline
			leftover = make([]byte, len(selected[lastNewLineIndex+1:]))
			copy(leftover, selected[lastNewLineIndex+1:])

			// Send the current chunk to the chunkStream channel
			chunkStream <- res
		}
		close(chunkStream) // Close the chunkStream channel to signal that no more data will be sent

		wg.Wait()           // Wait for all worker goroutines to finish
		close(citiesStream) // Close the citiesStream channel to signal that no more data will be received
	}()

	// Collect the processed weather data from the citiesStream channel and merge it into the final cities map
	for t := range citiesStream {
		for city, temp := range t {
			if weather, ok := cities[city]; ok {
				// If the city is already in the cities map, update the weather statistics
				weather.Count += temp.Count
				weather.Sum += temp.Sum
				if temp.Min < weather.Min {
					weather.Min = temp.Min
				}
				if temp.Max > weather.Max {
					weather.Max = temp.Max
				}
				cities[city] = weather
			} else {
				// If the city is not in the cities map, add a new entry
				cities[city] = temp
			}
		}
	}

	return cities, nil // Return the final cities map and no error
}

func ChunkReader(buf []byte, selected chan<- map[string]m.Weather) {
	// This function reads and processes a chunk of data from the input buffer.
	// It takes two arguments:
	// 1. buf: a byte slice containing the data to be processed.
	// 2. selected: a write-only channel of maps, where the processed data will be sent.

	var sb strings.Builder            // Declare a string builder to accumulate the city name
	res := make(map[string]m.Weather) // Declare a map to store the weather data for each city
	var city string                   // Declare a variable to store the current city name

	for _, char := range buf {
		// Iterate through each character in the input buffer
		if char == ';' {
			// If the character is a semicolon, it means we have reached the end of the city name
			city = sb.String() // Store the city name
			sb.Reset()         // Reset the string builder
		} else if char == '\n' {
			// If the character is a newline, it means we have reached the end of the weather data for the current city
			if sb.Len() != 0 && len(city) != 0 {
				// If the string builder and city name are not empty, process the weather data
				temp := u.ConvertStringToInt64(sb.String()) // Convert the temperature string to an integer
				sb.Reset()                                  // Reset the string builder

				if weather, ok := res[city]; ok {
					// If the city is already in the result map, update the weather data
					weather.Count++     // Increment the count
					weather.Sum += temp // Add the temperature to the sum
					if temp > weather.Max {
						weather.Max = temp // Update the maximum temperature
					}
					if temp < weather.Min {
						weather.Min = temp // Update the minimum temperature
					}
					res[city] = weather // Update the weather data in the result map
				} else {
					// If the city is not in the result map, add a new entry
					res[city] = m.Weather{Min: temp, Max: temp, Sum: temp, Count: 1}
				}
			}
		} else {
			// For any other character, append it to the string builder
			sb.WriteByte(char)
		}
	}
	selected <- res // Send the processed data to the output channel
}
