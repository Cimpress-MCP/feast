package feast.storage.connectors.jdbc.common;

import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import com.zaxxer.hikari.HikariDataSource;


@Configuration
public class JdbcConfig {

  @Bean
  public DataSource dataSource() {

     final HikariDataSource ds = new HikariDataSource();
     ds.setMaximumPoolSize(100);
     ds.setDriverClassName("oracle.jdbc.driver.OracleDriver"); 
     ds.setJdbcUrl("jdbc:oracle:thin:@localhost:1521:XE"); ;
     ds.setUsername("username");
     ds.setPassword("password");
     return ds;
  }

  @Bean
  public JdbcTemplate jdbcTemplate() {

      return new JdbcTemplate(dataSource());
  }
}
