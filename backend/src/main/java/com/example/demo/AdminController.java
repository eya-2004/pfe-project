package com.example.demo;

import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import java.util.Map;

@RestController
@RequestMapping("/admin")
public class AdminController {

	@Autowired
	private AgentHistoryService agentHistoryService;

	@Autowired
	private UserRepository userRepository;

	private final AuthService authService;

	public AdminController(AuthService authService) {
		this.authService = authService;
	}

	@GetMapping("/agents")
	public ResponseEntity<List<User>> getAgents() {
		return ResponseEntity.ok(authService.getAllAgents());
	}

	@GetMapping("/whoami")
	public ResponseEntity<?> whoami() {
		Authentication auth = SecurityContextHolder.getContext().getAuthentication();
		if (auth == null) return ResponseEntity.ok("auth is NULL");
		return ResponseEntity.ok(Map.of(
				"name", auth.getName(),
				"authorities", auth.getAuthorities().toString(),
				"authenticated", auth.isAuthenticated()
		));
	}

	@GetMapping("/agents/history-summary")
	public ResponseEntity<List<AgentIterationSummary>> getAgentsHistorySummary() {
		try {
			List<AgentIterationSummary> summary = agentHistoryService.getAllAgentsHistorySummary();
			return ResponseEntity.ok(summary);
		} catch (Exception e) {
			return ResponseEntity.internalServerError().build();
		}
	}

	@GetMapping("/agents/{id}/iterations")
	public ResponseEntity<List<IterationSummaryDto>> getAgentIterations(@PathVariable Long id) {
		try {
			List<IterationSummaryDto> iterations = agentHistoryService.getAgentIterations(id);
			return ResponseEntity.ok(iterations);
		} catch (Exception e) {
			return ResponseEntity.internalServerError().build();
		}
	}

	@PostMapping("/createAgent")
	public ResponseEntity<String> createAgent(
			@RequestParam String firstname,
			@RequestParam String lastname,
			@RequestParam String mail) {
		try {
			authService.createAgent(firstname, lastname, mail);
			return ResponseEntity.ok("Agent créé et identifiants envoyés!");
		} catch (Exception e) {
			return ResponseEntity.badRequest().body(e.getMessage());
		}
	}

	@DeleteMapping("/deleteagent/{id}")
	public ResponseEntity<String> deleteAgent(@PathVariable Long id) {
		try {
			authService.deleteAgent(id);
			return ResponseEntity.ok("Agent supprimé");
		} catch (Exception e) {
			return ResponseEntity.badRequest().body(e.getMessage());
		}
	}
}