package com.example.demo;

public class AgentIterationSummary {
    private Long id;
    private String firstname;
    private String lastname;
    private String email;
    private Integer totalDetectionIterations;
    private Integer totalCorrectionIterations;
    private String lastIterationDate;

    public AgentIterationSummary() {}

    public AgentIterationSummary(Long id, String firstname, String lastname, String email,
                                 Integer totalDetectionIterations, Integer totalCorrectionIterations,
                                 String lastIterationDate) {
        this.id = id;
        this.firstname = firstname;
        this.lastname = lastname;
        this.email = email;
        this.totalDetectionIterations = totalDetectionIterations;
        this.totalCorrectionIterations = totalCorrectionIterations;
        this.lastIterationDate = lastIterationDate;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getFirstname() { return firstname; }
    public void setFirstname(String firstname) { this.firstname = firstname; }

    public String getLastname() { return lastname; }
    public void setLastname(String lastname) { this.lastname = lastname; }

    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }

    public Integer getTotalDetectionIterations() { return totalDetectionIterations; }
    public void setTotalDetectionIterations(Integer v) { this.totalDetectionIterations = v; }

    public Integer getTotalCorrectionIterations() { return totalCorrectionIterations; }
    public void setTotalCorrectionIterations(Integer v) { this.totalCorrectionIterations = v; }

    public String getLastIterationDate() { return lastIterationDate; }
    public void setLastIterationDate(String lastIterationDate) { this.lastIterationDate = lastIterationDate; }
}