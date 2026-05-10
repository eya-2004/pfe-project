package com.example.demo;
import org.springframework.stereotype.Repository;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
//Pour retourner une liste d'utilisateurs
import java.util.Optional;
//Conteneur qui peut contenir une valeur ou être vide
//Evite NullPointerException
@Repository
public interface UserRepository extends JpaRepository<User,Long>{
	Optional<User> findByMail(String mail);
	List<User> findByRole(Role role);
	boolean existsByMail (String mail);//return true if  mail exist

}
