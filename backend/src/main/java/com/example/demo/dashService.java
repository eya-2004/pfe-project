package com.example.demo;
import org.springframework.stereotype.*;

import java.util.Map;
import java.util.List;
import java.util.LinkedHashMap;
import org.springframework.beans.factory.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
@Service
public class dashService {
	@Autowired
	private JdbcTemplate jdbcTemplate;
	public Stats getStats() {
		System.out.println("Télechargement depuis mysql");
		try {
			String query = "select count(*) from bscs_findocs" ;
			int total=jdbcTemplate.queryForObject(query,Integer.class);
			System.out.println("total des docs "+total);
			int errors=jdbcTemplate.queryForObject("select count(*) from bscs_findocs_errors",Integer.class);//type de retour integer
			System.out.println("total des erreurs"+errors);
			int success=jdbcTemplate.queryForObject(
					"select count(DISTINCT f.id_findoc) from bscs_findocs f"
					+ " WHERE NOT EXISTS(select 1 from bscs_findocs_errors e  where e.id_findoc=f.id_findoc)"
					,Integer.class);
			System.out.println("Total des données valides "+success);
			float scoreSucces = total >0 ?((float) success /total * 100) : 0;
			System.out.println("Pourcentage de succès"+scoreSucces);
			float scoreError =total >0 ?((float) errors /total * 100) : 0;
			System.out.println("Pourcentage d'erreur"+scoreError);
			return new  Stats(total,errors,success,scoreSucces);
		}catch (Exception e){
			System.out.println("erreur"+e.getMessage());
			e.printStackTrace();
			return new Stats(0,0,0,0);
		}
	}
	public Map<String,Integer> getErreurs(){
		 Map<String,Integer> erreurs=new LinkedHashMap<>();//insertion des erreurs par ordre
		 try {
		  List<Map<String,Object>> erreurParType=jdbcTemplate.queryForList("select error_type ,count(*)  as nombre from bscs_findocs_errors"
		 		+ " GROUP BY ERROR_TYPE"+ " order by nombre desc "); // yrajaali listee feha plusieurs ligne chaque ligne est une map object nimporte quell type de donnees
		  
		 for (Map<String,Object>  row :erreurParType) {
			 String type =(String) row.get("ERROR_TYPE");
			 Integer count=((Number) row.get("nombre")).intValue();
		  erreurs.put(type, count);			 
			 
		 }
		 }catch(Exception e) {
			 System.out.println("erreur "+e.getMessage());
		 }return erreurs;
	}
	public int getSucces() {
		Stats state=getStats();
		return state.getTotalsucces();
	}
	public String getDecision() {
		Stats state =getStats();
		float score=state.getScore();
		 if (score >= 90) {
			 return "Données à migrer";
		 }else if (score >= 60){
			return "Données à nettoyer";
		 }else 
			 return "Données à rejetter";
	}
	

}
