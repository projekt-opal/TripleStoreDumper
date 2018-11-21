package de.upb.cs.dice.triplestoredump;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface InfoDataSetRepository extends CrudRepository<InfoDataSet, Long> {
    List<InfoDataSet> findByTitleAndPortal(String string, String portal);

    Optional<InfoDataSet> findByUri(String uri);
}
