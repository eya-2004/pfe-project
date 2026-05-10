package com.example.demo;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import java.util.Map;
@RestController
@RequestMapping("/agent")
public class AgentController {
	private final AuthService  authService;
	public AgentController(AuthService  authService) {
		this.authService=authService;
	}
	@PostMapping("/change-password")
	public ResponseEntity<String> changePasword(@RequestBody Map<String, String> body){
		try {
			authService.changePassword(body .get("mail"), body .get("oldPassword"), body .get("newPassword"));
			return ResponseEntity.ok("Mot de passe changé avec succès!");
		}catch (Exception  e ) {
			return ResponseEntity.badRequest().body(e.getMessage());
		}
	}

}
