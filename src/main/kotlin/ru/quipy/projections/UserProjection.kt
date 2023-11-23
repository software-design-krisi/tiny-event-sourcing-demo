package ru.quipy.projections

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Component
import org.springframework.stereotype.Repository
import ru.quipy.api.*
import ru.quipy.core.EventSourcingService
import ru.quipy.logic.UserAggregateState
import ru.quipy.streams.AggregateSubscriptionsManager
import java.util.*
import javax.annotation.PostConstruct

@Component
class UserProjection (
    private val userRepository: UserRepository,
    private val subscriptionsManager: AggregateSubscriptionsManager,
    private val userEsService: EventSourcingService<UUID, UserAggregate, UserAggregateState>
){
    private val logger = LoggerFactory.getLogger(UserProjection::class.java)

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(UserAggregate::class, "users::user-projection") {
            `when`(UserCreatedEvent::class) { event ->
                withContext(Dispatchers.IO) {
                    userRepository.save(User(event.userId, event.userName, event.nickname))
                }
                logger.info("Update user projection, create user ${event.userId}")
            }
        }
    }

    fun login(nickname: String, password: String) : User {
        val user = userRepository.findAll().single{ x -> x.nickname == nickname} ?: throw Exception("Invalid nickname")
        val userAggregate = userEsService.getState(user.userId) ?: throw Exception("Fail to get aggregate")
        if (userAggregate.password == password)
            return user
        else
            throw Exception("Invalid password")
    }

    fun getAll() : List<User> {
        return userRepository.findAll()
    }

    fun getById(id: UUID) : User? {
        return userRepository.findByIdOrNull(id)
    }

    fun getByNickname(nickname: String) : User? {
        val users = userRepository.findAll()
        if (users.isNotEmpty())
            return users.single{x -> x.nickname == nickname}
        return null
    }
}

@Document("user-projection")
data class User(
    @Id
    var userId: UUID,
    val name: String,
    val nickname: String
)

@Repository
interface UserRepository : MongoRepository<User, UUID>