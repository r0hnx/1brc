package utils

import (
	models "1brc/models"
	"fmt"
	"math"
	"os"
	"strconv"
)

func ConvertStringToInt64(input string) int64 {
	// input = "-40.2", len = 5
	// input = "-40" + "2" = "-402"
	input = input[:len(input)-2] + input[len(input)-1:]
	output, _ := strconv.ParseInt(input, 10, 64)
	return output
}

func WriteWeatherStats(cityWeather map[string]models.Weather) {
	file, err := os.Create("cities.out")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	for name, city := range cityWeather {
		_, err = fmt.Fprintf(file, "%s=%d/%.1f/%d\n", name, city.Min, math.Round(float64(city.Sum/city.Count)), city.Max)
		if err != nil {
			fmt.Println(err)
		}
	}
}
