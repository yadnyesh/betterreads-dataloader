package io.yadnyesh.betterreads.dataloader;

import io.yadnyesh.betterreads.dataloader.connection.DataStaxAstraProperties;
import io.yadnyesh.betterreads.dataloader.models.Book;
import io.yadnyesh.betterreads.dataloader.repository.BookRepository;
import lombok.extern.slf4j.Slf4j;
import io.yadnyesh.betterreads.dataloader.models.Author;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import io.yadnyesh.betterreads.dataloader.repository.AuthorRepository;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
@Slf4j
public class BetterreadsDataloaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	@Autowired
	BookRepository bookRepository;

	@Value("${datadump.location.author}")
	private String authorDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataloaderApplication.class, args);
	}

	private void initAuthors() {
		Path authorFilePath = Paths.get(authorDumpLocation);
		try {
			Stream<String> lines = Files.lines(authorFilePath);
			lines.forEach( line -> {
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);
					Author author = new Author();
					author.setName(jsonObject.optString("name"));
					author.setPersonalName(jsonObject.optString("name"));
					author.setId(jsonObject.optString("key").replace("/authors", ""));
					authorRepository.save(author);
					log.info("Saved information for Author: " + author.getName());
				} catch (JSONException e) {
					log.error(e.getMessage());
				}
			});
		} catch (IOException e) {
			log.error(e.getMessage());
		}
	}

	private void initWorks() {
		Path worksFilePath = Paths.get(worksDumpLocation);
		DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		try {
			Stream<String> lines = Files.lines(worksFilePath);
			lines.forEach( line -> {
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);
					Book book = new Book();
					book.setId(jsonObject.getString("key").replace("/works/", ""));
					book.setName(jsonObject.optString("title"));
					JSONObject jsonDescriptionObject = jsonObject.optJSONObject("description");
					if(jsonDescriptionObject != null) {
						book.setDescription(jsonDescriptionObject.optString("value"));
					}
					JSONObject publishedObj = jsonObject.optJSONObject("created");
					if(publishedObj != null) {
						String dateStr = publishedObj.getString("value");
						book.setPublishedDate(LocalDate.parse(dateStr, dateTimeFormatter));
					}
					JSONArray coversJSONArray = jsonObject.optJSONArray("covers");
					if(coversJSONArray != null) {
						List<String> coverIds = new ArrayList<>();
						for(int i = 0; i < coversJSONArray.length(); i++){
							coverIds.add(coversJSONArray.getString(i));
						}
						book.setCoverIds(coverIds);
					}
					JSONArray authorsJSONArray = jsonObject.optJSONArray("authors");
					if(authorsJSONArray != null) {
						List<String> authorIds = new ArrayList<>();
						for (int i = 0; i < authorsJSONArray.length(); i++) {
							String authorId = authorsJSONArray.getJSONObject(i)
									        .getJSONObject("author")
									        .getString("key")
									.replace("/authors/","");
							authorIds.add(authorId);
						}
						book.setAuthorIds(authorIds);
						List<String> authorNames = authorIds.stream()
								.map(id -> authorRepository.findById(id))
								.map(optionalAuthor -> {
									if (optionalAuthor.isEmpty()) {
										return "unknown author";
									}
									return optionalAuthor.get().getName();

								}).collect(Collectors.toList());
						book.setAuthorNames(authorNames);
					}
					bookRepository.save(book);
					log.info("Saved information for Book: " + book.getName());
				} catch (JSONException e) {
					log.error(e.getMessage());
				}

			});
		} catch (IOException e) {
			log.error(e.getMessage());
		}
	}

	@PostConstruct
	public void startUp(){
		log.info("**********************Application Started**********************");

//		Author author = new Author();
//		author.setId("myId");
//		author.setName("yadnyesh");
//		author.setPersonalName("ybj");
//		authorRepository.save(author);
//		log.info("Author record created");
		log.info(authorDumpLocation);
		log.info(worksDumpLocation);
//		initAuthors();
//		initWorks();
	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return cqlSessionBuilder -> cqlSessionBuilder.withCloudSecureConnectBundle(bundle);
	}

}
