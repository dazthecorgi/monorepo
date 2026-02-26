package main

import "sync"

type ProjectRegistry struct {
	mu       sync.Mutex
	projects map[string]bool
}

func NewProjectRegistry() *ProjectRegistry {
	return &ProjectRegistry{
		projects: make(map[string]bool),
	}
}

func (pr *ProjectRegistry) Register(projectName string) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.projects[projectName] = true
}

func (pr *ProjectRegistry) Unregister(projectName string) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	delete(pr.projects, projectName)
}

func (pr *ProjectRegistry) GetAll() []string {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	var result []string
	for name := range pr.projects {
		result = append(result, name)
	}
	return result
}
