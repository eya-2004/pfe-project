package com.example.demo;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.stereotype.Service;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
@Service
public class UserDetailsServiceImpl implements  UserDetailsService {
	private final UserRepository userRepository;
	// Constructeur : Spring injecte automatiquement UserRepository
	public UserDetailsServiceImpl(UserRepository userRepository) {
		this.userRepository=userRepository;
	}
	@Override
	public UserDetails loadUserByUsername(String mail) throws UsernameNotFoundException{
		return userRepository.findByMail(mail)
				.orElseThrow(()-> new UsernameNotFoundException("Utilisateur non trouvé"));
	}
	
	
}
