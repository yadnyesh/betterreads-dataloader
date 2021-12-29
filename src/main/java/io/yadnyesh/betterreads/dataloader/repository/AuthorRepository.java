package io.yadnyesh.betterreads.dataloader.repository;

import io.yadnyesh.betterreads.dataloader.models.Author;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AuthorRepository extends CassandraRepository<Author, String> {

}
