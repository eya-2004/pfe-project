package com.example.demo;
import jakarta.persistence.*;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import java.util.Collection;
import java.util.List;
import java.time.LocalDateTime;
@Entity
@Table(name="users")

public class User implements UserDetails {
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	private long id;
	private String firstname;
	private String lastname;
    private String dateNaissance;

	@Column(unique=true,nullable=false)
	private String mail;
	private String password;
	@Enumerated(EnumType.STRING)
	private Role role;
	private boolean mustChangePassword;
    public User() {}

    public User(Long id, String firstname, String lastname,String dateNaissance,
                String mail, String password,
                Role role, boolean mustChangePassword) {
        this.id = id;
        this.firstname = firstname;
        this.lastname = lastname;
        this.dateNaissance=dateNaissance;
        this.mail = mail;
        this.password = password;
        this.role = role;
        this.mustChangePassword = mustChangePassword;
    }

    // -------- Getters --------
    public Long getId()                   { return id; }
    public String getFirstname()          { return firstname; }
    public String getLastname()           { return lastname; }
    public String getDateNaissance()   {return dateNaissance;}
    public String getMail()               { return mail; }
    public Role getRole()                 { return role; }
    public boolean isMustChangePassword() { return mustChangePassword; }

   
    public void setId(Long id)                      { this.id = id; }
    public void setFirstname(String firstname)      { this.firstname = firstname; }
    public void setLastname(String lastname)        { this.lastname = lastname; }
    public void setDateNaissance(String dateNaissance){this.dateNaissance=dateNaissance;}
    public void setMail(String mail)                { this.mail = mail; }
    public void setPassword(String password)        { this.password = password; }
    public void setRole(Role role)                  { this.role = role; }
    public void setMustChangePassword(boolean mcp) { this.mustChangePassword = mcp; }

	@Override
	public Collection<? extends GrantedAuthority> getAuthorities() {

        return List.of(new SimpleGrantedAuthority("ROLE_" + role.name()));
	}
        @Override 
        public String getUsername() {
            return mail; // on utilise l'email comme identifiant de connexion
        }
        @Override
        public String getPassword() {
            return password;
        }
        @Override
        public boolean isAccountNonExpired() { return true; }

        @Override
        public boolean isAccountNonLocked() { return true; }

        @Override
        public boolean isCredentialsNonExpired() { return true; }

        @Override
        public boolean isEnabled() { return
        		true; }


}