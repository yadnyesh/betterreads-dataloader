package io.yadnyesh.betterreads.dataloader.repository;

import io.yadnyesh.betterreads.dataloader.models.Book;
import org.springframework.data.cassandra.repository.CassandraRepository;

public interface BookRepository extends CassandraRepository<Book, String> {

}
