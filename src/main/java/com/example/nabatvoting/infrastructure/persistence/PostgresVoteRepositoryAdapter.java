package com.example.nabatvoting.infrastructure.persistence;

import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.model.Vote;
import com.example.nabatvoting.domain.model.VoteCounts;
import com.example.nabatvoting.domain.model.VoteId;
import com.example.nabatvoting.domain.model.VoteType;
import com.example.nabatvoting.domain.model.VoterId;
import com.example.nabatvoting.domain.port.out.VoteRepository;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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
    public VoteCounts countsFor(AlertId alertId) {
        Map<VoteType, Long> tallies = jpaRepository.tallyByAlertId(alertId.value()).stream()
                .collect(Collectors.toMap(
                        VoteJpaRepository.VoteTypeTally::getVoteType,
                        VoteJpaRepository.VoteTypeTally::getTotal));

        return new VoteCounts(
                countOf(tallies, VoteType.UPVOTE),
                countOf(tallies, VoteType.DOWNVOTE),
                countOf(tallies, VoteType.CONFIRM)
        );
    }

    private static int countOf(Map<VoteType, Long> tallies, VoteType type) {
        return Math.toIntExact(tallies.getOrDefault(type, 0L));
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
