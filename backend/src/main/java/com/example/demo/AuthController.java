package com.example.demo;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.context.SecurityContextRepository;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.security.authentication.BadCredentialsException;

import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;

import java.util.Map;
import java.util.HashMap;

@RestController
@RequestMapping("api/auth")
public class AuthController {
    private final UserDetailsService userDetailsService;
    private final AuthService authService;
    private final AuthenticationManager authenticationManager;
    private final SecurityContextRepository securityContextRepository;

    public AuthController(AuthService authService, UserDetailsService userDetailsService,
                          AuthenticationManager authenticationManager,
                          SecurityContextRepository securityContextRepository) {
        this.authService = authService;
        this.authenticationManager = authenticationManager;
        this.securityContextRepository = securityContextRepository;
        this.userDetailsService = userDetailsService;
    }

    @PostMapping("/login")
    public ResponseEntity<?> login(@RequestParam String mail,
                                   @RequestParam String password,
                                   HttpServletRequest request,
                                   HttpServletResponse response) {
        try {
            // Validation du format email
            if (!isValidEmail(mail)) {
                Map<String, String> errorResponse = new HashMap<>();
                errorResponse.put("error", "Format email incorrect");
                return ResponseEntity.status(400).body(errorResponse);
            }

            Authentication authentication = authenticationManager.authenticate(
                    new UsernamePasswordAuthenticationToken(mail, password)
            );

            // Create a new context and save it properly
            SecurityContext context = SecurityContextHolder.createEmptyContext();
            context.setAuthentication(authentication);
            SecurityContextHolder.setContext(context);
            securityContextRepository.saveContext(context, request, response);

            User user = (User) authentication.getPrincipal();
            String role = authentication.getAuthorities()
                    .iterator().next()
                    .getAuthority();

            Map<String, Object> resp = new HashMap<>();
            resp.put("role", role);
            resp.put("mail", user.getMail());
            resp.put("mustChangePassword", user.isMustChangePassword());
            resp.put("message", "Connexion réussie");
            return ResponseEntity.ok(resp);

        } catch (BadCredentialsException e) {
            // Distinguer entre utilisateur non trouvé et mauvais mot de passe
            Map<String, String> errorResponse = new HashMap<>();

            // Vérifier d'abord si l'utilisateur existe
            try {
                UserDetails userDetails = userDetailsService.loadUserByUsername(mail);
                // Si on arrive ici, l'utilisateur existe mais mauvais mot de passe
                errorResponse.put("error", "Mot de passe incorrect");
            } catch (UsernameNotFoundException ex) {
                // Utilisateur non trouvé
                errorResponse.put("error", "Utilisateur non trouvé");
            }

            return ResponseEntity.status(401).body(errorResponse);

        } catch (Exception e) {
            e.printStackTrace();
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("error", "Erreur interne du serveur");
            return ResponseEntity.status(500).body(errorResponse);
        }
    }

    // Méthode utilitaire pour valider le format email
    private boolean isValidEmail(String email) {
        String emailRegex = "^[a-zA-Z0-9_+&*-]+(?:\\.[a-zA-Z0-9_+&*-]+)*@(?:[a-zA-Z0-9-]+\\.)+[a-zA-Z]{2,7}$";
        Pattern pattern = Pattern.compile(emailRegex);
        if (email == null) return false;
        Matcher matcher = pattern.matcher(email);
        return matcher.matches();
    }

    @PostMapping("/registerAdmin")
    public ResponseEntity<String> registerAdmin(
            @RequestParam String firstname,
            @RequestParam String lastname,
            @RequestParam String mail,
            @RequestParam String password) {
        try {
            authService.registerAdmin(firstname, lastname, mail, password);
            return ResponseEntity.ok("Compte admin créé avec succès.");
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }
}