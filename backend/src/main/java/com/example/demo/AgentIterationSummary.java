package com.example.demo;


public class AgentIterationSummary {
    private Long id;
    private String firstname;
    private String lastname;
    private String email;
    private Integer totalIterations;      // Nombre total d'itérations
    private String lastIterationDate;     // Date de la dernière itération

    // Constructeurs
    public AgentIterationSummary() {}

    public AgentIterationSummary(Long id, String firstname, String lastname, String email,
                                 Integer totalIterations, String lastIterationDate) {
        this.id = id;
        this.firstname = firstname;
        this.lastname = lastname;
        this.email = email;
        this.totalIterations = totalIterations;
        this.lastIterationDate = lastIterationDate;
    }

    // Getters et Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getFirstname() { return firstname; }
    public void setFirstname(String firstname) { this.firstname = firstname; }

    public String getLastname() { return lastname; }
    public void setLastname(String lastname) { this.lastname = lastname; }

    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }

    public Integer getTotalIterations() { return totalIterations; }
    public void setTotalIterations(Integer totalIterations) { this.totalIterations = totalIterations; }

    public String getLastIterationDate() { return lastIterationDate; }
    public void setLastIterationDate(String lastIterationDate) { this.lastIterationDate = lastIterationDate; }
}