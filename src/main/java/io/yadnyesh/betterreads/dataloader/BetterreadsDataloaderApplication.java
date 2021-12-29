package io.yadnyesh.betterreads.dataloader;

import io.yadnyesh.betterreads.dataloader.connection.DataStaxAstraProperties;
import lombok.extern.slf4j.Slf4j;
import io.yadnyesh.betterreads.dataloader.models.Author;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import io.yadnyesh.betterreads.dataloader.repository.AuthorRepository;

import javax.annotation.PostConstruct;
import java.nio.file.Path;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
@Slf4j
public class BetterreadsDataloaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataloaderApplication.class, args);
	}

	@PostConstruct
	public void startUp(){
		log.info("**********************Application Started**********************");

		Author author = new Author();
		author.setId("myId");
		author.setName("yadnyesh");
		author.setPersonalName("ybj");
		authorRepository.save(author);
		log.info("Author record created");
	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return cqlSessionBuilder -> cqlSessionBuilder.withCloudSecureConnectBundle(bundle);
	}

}
