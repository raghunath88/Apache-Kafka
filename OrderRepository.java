package com.dwp.kafka.repository;

import com.dwp.kafka.model.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface OrderRepository extends JpaRepository<OrderEntity, Long> {

    boolean existsByEventId(String eventId);

    Optional<OrderEntity> findByOrderId(String orderId);

    List<OrderEntity> findByUserIdOrderByCreatedAtDesc(String userId);

    List<OrderEntity> findByUserIdAndStatus(String userId, String status);
}
