package com.example.demo;
import java.util.List;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import java.security.SecureRandom;
import java.time.LocalDateTime;
@Service
public class AuthService {
	private final UserRepository userRepository;
	private final PasswordEncoder passEncoder;
	private final JavaMailSender javaMailSender;
	public AuthService(UserRepository userRepository,PasswordEncoder passEncoder,JavaMailSender javaMailSender) {
		this.javaMailSender=javaMailSender;
		this.passEncoder=passEncoder;
		this.userRepository=userRepository;
		
	}

	public void createAgent(String firstname,String lastname,String dateNaissance,
            String mail) {
		if(userRepository.existsByMail (mail))
			throw new RuntimeException("email déja utilisé");
		String tempPass=generatePassword();
		User agent=new User();
		agent.setFirstname(firstname);
		agent.setLastname(lastname);
		agent.setDateNaissance(dateNaissance);
		agent.setMail(mail);
		agent.setPassword(passEncoder.encode(tempPass));
		agent.setRole(Role.AGENT_MIGRATION);
		agent.setMustChangePassword(true);
		userRepository.save(agent);
		
		try {
	        sendCredentials(mail, tempPass);
	    } catch (Exception e) {
	        System.err.println("❌ Erreur d'envoi des identifiants : " + e.getMessage());
	    }
		
	}
	public void deleteAgent(Long id) {
		userRepository.deleteById(id);
	}
	public List<User> getAllAgents(){
		return userRepository.findByRole(Role.AGENT_MIGRATION);
	}
	public void changePassword (String mail ,String oldPass,String newPass) {
		User user = userRepository.findByMail(mail)
                .orElseThrow(() -> new RuntimeException("Utilisateur non trouvé."));
		if(!passEncoder.matches( oldPass,user.getPassword()))
			throw new RuntimeException("ancien mot de passe incorrect");
		user.setPassword(passEncoder.encode(newPass));
		user.setMustChangePassword(false);
		userRepository.save(user);
	}
	public User findByMail(String mail) {
		return userRepository.findByMail(mail).orElse(null);
	}
	private void sendCredentials(String mail,String password) {
		SimpleMailMessage msg=new SimpleMailMessage();
		msg.setTo(mail);
		msg.setSubject("vos identifiants de connexion");
		msg.setText("Salut,\n\n" +
		        "Votre compte agent a été créé.\n\n" +
		        "Email    : " + mail + "\n" +
		        "Mot de passe temporaire : " + password + "\n\n" +
		        "Connectez-vous ici : http://localhost:3000/login\n\n" +
		        "Vous serez invité à changer votre mot de passe à la première connexion.\n\n" +
		        "Cordialement.");
		javaMailSender.send(msg);
	}
	 private String generatePassword() {
	        String chars = "ABCDEFGHIJabcdefghij0123456789@#!";
			  // Générateur aléatoire sécurisé
	        SecureRandom r = new SecureRandom();
	        StringBuilder sb = new StringBuilder();
			//  mot de passe de 10 caractères

	        for (int i = 0; i < 10; i++)
	            sb.append(chars.charAt(r.nextInt(chars.length())));
	       
	        return sb.toString();
	    }
	

}
