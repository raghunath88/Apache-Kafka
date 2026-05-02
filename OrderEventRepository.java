package com.dwp.kafka.repository;

import com.dwp.kafka.model.OrderEventDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public interface OrderEventRepository extends MongoRepository<OrderEventDocument, String> {

    Optional<OrderEventDocument> findByEventId(String eventId);

    List<OrderEventDocument> findByUserIdOrderByTimestampDesc(String userId);

    List<OrderEventDocument> findByUserIdAndTimestampBetween(String userId, Instant from, Instant to);

    boolean existsByEventId(String eventId);
}
