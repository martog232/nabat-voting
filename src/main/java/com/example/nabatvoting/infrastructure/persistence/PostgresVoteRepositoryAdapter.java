package com.example.nabatvoting.infrastructure.persistence;

import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.model.Vote;
import com.example.nabatvoting.domain.model.VoteId;
import com.example.nabatvoting.domain.model.VoterId;
import com.example.nabatvoting.domain.port.out.VoteRepository;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Primary
@Component
public class PostgresVoteRepositoryAdapter implements VoteRepository {

    private final VoteJpaRepository jpaRepository;

    public PostgresVoteRepositoryAdapter(VoteJpaRepository jpaRepository) {
        this.jpaRepository = jpaRepository;
    }

    @Override
    public void save(Vote vote) {
        VoteJpaEntity entity = new VoteJpaEntity(
                vote.getId().value(),
                vote.getAlertId().value(),
                vote.getVoterId().value(),
                vote.getVoteType(),
                vote.getCastAt()
        );
        jpaRepository.save(entity);
    }

    @Override
    public void deleteByAlertIdAndVoterId(AlertId alertId, VoterId voterId) {
        jpaRepository.deleteByAlertIdAndVoterId(alertId.value(), voterId.value());
    }

    @Override
    public Optional<Vote> findByAlertIdAndVoterId(AlertId alertId, VoterId voterId) {
        return jpaRepository.findByAlertIdAndVoterId(alertId.value(), voterId.value())
                .map(this::toDomain);
    }

    @Override
    public int countUpvotes(AlertId alertId) {
        return jpaRepository.countUpvotesByAlertId(alertId.value());
    }

    @Override
    public int countDownvotes(AlertId alertId) {
        return jpaRepository.countDownvotesByAlertId(alertId.value());
    }

    @Override
    public int countConfirmations(AlertId alertId) {
        return jpaRepository.countConfirmationsByAlertId(alertId.value());
    }

    @Override
    public List<AlertId> findDistinctAlertIds() {
        return jpaRepository.findDistinctAlertIds().stream()
                .map(AlertId::new)
                .toList();
    }

    private Vote toDomain(VoteJpaEntity entity) {
        return new Vote(
                new VoteId(entity.getId()),
                new AlertId(entity.getAlertId()),
                new VoterId(entity.getVoterId()),
                entity.getVoteType(),
                entity.getCastAt()
        );
    }
}
