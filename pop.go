package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
)

type Population struct {
	State      string
	City       string
	Population int
}

type Worker struct {
	Files []string
}

func main() {
	// check that args in correct format
	if len(os.Args) != 3 {
		fmt.Println("Usage: go run pop.go <tar_file> <min_pop>")
		return
	}

	// stores the args as variables
	dir := os.Args[1]
	min_pop, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println("Invalid population value:", os.Args[2])
		return
	}

	// create channels for each worker to send their results
	results := make(chan []Population)

	// WaitGroup to wait for all workers to finish
	var wg sync.WaitGroup

	// create the workers
	worker0 := Worker{Files: []string{"cities1.csv", "cities2.csv", "cities3.csv"}}
	worker1 := Worker{Files: []string{"cities4.csv", "cities5.csv", "cities6.csv"}}
	worker2 := Worker{Files: []string{"cities7.csv", "cities8.csv", "cities9.csv", "cities10.csv"}}

	// each worker runs map in its own goroutine
	wg.Add(3)
	go func() {
		defer wg.Done()
		results <- Map(worker0, dir, min_pop)
	}()
	go func() {
		defer wg.Done()
		results <- Map(worker1, dir, min_pop)
	}()
	go func() {
		defer wg.Done()
		results <- Map(worker2, dir, min_pop)
	}()

	// collect all the results from the workers
	var allPopulations []Population
	go func() {
		wg.Wait()      // Wait for all workers to complete
		close(results) // Close the results channel once all workers are done
	}()
	for populationList := range results {
		allPopulations = append(allPopulations, populationList...)
	}

	// Reduce function in a separate goroutine
	var reduceWg sync.WaitGroup
	reduceWg.Add(1)

	var output string
	go func() {
		defer reduceWg.Done()
		output = reduce(allPopulations)
	}()

	// Wait for the reduce function to finish
	reduceWg.Wait()

	// Print the final output
	fmt.Println(output)
}

/*
Map function: Each worker goroutine should search the file it is assigned to and filter out
the records of cities in the file that have populations ≥ min_pop.
*/
func Map(worker Worker, dir string, min_pop int) []Population {
	var populations []Population
	for _, file := range worker.Files {
		filePath := fmt.Sprintf("%s/%s", dir, file)

		//calls readCSV and converts into an array of type Population
		filePopulations, err := readCSV(filePath)
		if err != nil {
			fmt.Println("Error reading file:", file, err)
			continue
		}

		//appends to populations only if its above min_pop
		for _, pop := range filePopulations {
			if pop.Population >= min_pop {
				populations = append(populations, pop)
			}
		}
	}

	return populations
}

// reads csvs and makes a Population object for each item, returns array of these items
func readCSV(filePath string) ([]Population, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	// creates new population objects for each item in records
	var populations []Population
	for _, record := range records {
		city := record[0]
		state := record[1]
		population, err := strconv.Atoi(record[2])
		if err != nil {
			return nil, err
		}
		populations = append(populations, Population{
			State:      state,
			City:       city,
			Population: population,
		})
	}
	return populations, nil
}

/*
Reduce function: A separate goroutine should take the records produced by the worker
goroutines and reduce them to a list of states and their cities with populations ≥ min_pop.
The goroutine should then output the results in the format illustrated in the sample output
above.
*/
func reduce(pop_list []Population) string {
	output := ""

	//create a map for each of the states and sort by their frequency
	stateCount := countStateFrequency(pop_list)
	sortedStates := sortStatesByFrequency(stateCount)

	// create a map to group populations by their state
	stateMap := make(map[string][]Population)

	// group cities under their respective states
	for _, pop := range pop_list {
		stateMap[pop.State] = append(stateMap[pop.State], pop)
	}

	// generate the formatted string with frequency in state header
	for _, state := range sortedStates {
		stateFrequency := stateCount[state]
		output += fmt.Sprintf("%s: %d\n", state, stateFrequency)
		for _, city := range stateMap[state] {
			output += fmt.Sprintf("- %s, %d\n", city.City, city.Population)
		}
	}

	return output
}

// counts occurrences of each state in the population list
func countStateFrequency(pop_list []Population) map[string]int {
	stateCount := make(map[string]int)
	for _, pop := range pop_list {
		stateCount[pop.State]++
	}
	return stateCount
}

// sort states by frequency (highest to lowest)
func sortStatesByFrequency(stateCount map[string]int) []string {
	var states []string
	for state := range stateCount {
		states = append(states, state)
	}
	sort.Slice(states, func(i, j int) bool {
		return stateCount[states[i]] > stateCount[states[j]]
	})
	return states
}
