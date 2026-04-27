package com.example.demo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
@RestController
@RequestMapping("/dashboard")
@CrossOrigin(origins="http://localhost:5173")
public class dashController {
	@Autowired
	private dashService dashSce;
	@GetMapping("/stats")
	public Stats getStats() {
		return dashSce.getStats();
	}
	@GetMapping("/errors")
	public Map<String,Integer> getErreurs(){
		Map<String,Integer> result=dashSce.getErreurs();
		System.out.println("les erreurs "+result);
		return result;
	}
	@GetMapping("/success")
	public int getSuccess() {
		return dashSce.getSucces();
	}
	@GetMapping("/decisions")
	public String getDecision() {
		return dashSce.getDecision();
	}
	

}
