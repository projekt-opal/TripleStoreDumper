package de.upb.cs.dice.triplestoredump;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface InfoDataSetRepository extends CrudRepository<InfoDataSet, Long> {
    Optional<InfoDataSet> findByTitleAndPortal(String string, String portal);
}
