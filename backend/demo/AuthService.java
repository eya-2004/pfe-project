package com.example.demo;
import java.util.List;
import org.springframework.mail.SimpleMailMessage;
//Classe pour créer un email simple en texte brut
import org.springframework.mail.javamail.JavaMailSender;
//Interface Spring pour envoyer des emails via SMTP

import org.springframework.security.crypto.password.PasswordEncoder;
//Pour hasher les mots de passe avant stockage en base
import org.springframework.stereotype.Service;
//Marque cette classe comme un Service Spring

import java.security.SecureRandom;
//Générateur aléatoire sécurisé pour les mots de passe temporaires
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
	public void registerAdmin(String firstname,String lastname,
            String mail, String password) {
		if (userRepository.existsByMail(mail))
			throw new RuntimeException("email déja utilisé");
		User admin=new User();
		admin.setFirstname(firstname);
		admin.setLastname(lastname);
		admin.setMail(mail);
		admin.setPassword(passEncoder.encode(password));
		admin.setRole(Role.ADMIN);
		admin.setMustChangePassword(false);
		userRepository.save(admin);
		
	}
	public void createAgent(String firstname,String lastname,
            String mail) {
		if(userRepository.existsByMail (mail))
			throw new RuntimeException("email déja utilisé");
		String tempPass=generatePassword();
		User agent=new User();
		agent.setFirstname(firstname);
		agent.setLastname(lastname);
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
		        "Connectez-vous ici : http://localhost:5173/login\n\n" +
		        "Vous serez invité à changer votre mot de passe à la première connexion.\n\n" +
		        "Cordialement.");
		javaMailSender.send(msg);
	}
	 private String generatePassword() {
	        String chars = "ABCDEFGHIJabcdefghij0123456789@#!";
	        // Caractères autorisés dans le mot de passe

	        SecureRandom r = new SecureRandom();
	        // Générateur aléatoire sécurisé

	        StringBuilder sb = new StringBuilder();
	        // Pour construire la chaîne caractère par caractère

	        for (int i = 0; i < 10; i++)
	            sb.append(chars.charAt(r.nextInt(chars.length())));
	        // Répète 10 fois → mot de passe de 10 caractères

	        return sb.toString();
	        // Retourne le mot de passe généré
	    }
	

}
