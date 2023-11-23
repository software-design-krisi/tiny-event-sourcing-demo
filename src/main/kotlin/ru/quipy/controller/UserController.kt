package ru.quipy.controller

import org.springframework.web.bind.annotation.*
import ru.quipy.api.UserAggregate
import ru.quipy.api.UserCreatedEvent
import ru.quipy.core.EventSourcingService
import ru.quipy.logic.UserAggregateState
import ru.quipy.logic.create
import ru.quipy.projections.*
import java.util.*

@RestController
@RequestMapping("/users")
class UserController (val userEsService: EventSourcingService<UUID, UserAggregate, UserAggregateState>,
                val userProjection: UserProjection,
                val userProjectsProjection: UserProjectsProjection
) {
    @PostMapping
    fun createUser(@RequestParam name: String, @RequestParam nickname: String, @RequestParam password: String) : UserCreatedEvent {
        if (userProjection.getByNickname(nickname) != null)
            throw Exception("User with nickname $nickname already exists")
        return userEsService.create{ it.create(UUID.randomUUID(), name, nickname, password) }
    }

    @PostMapping("login")
    fun login(@RequestParam nickname: String, @RequestParam password: String) : User {
        return userProjection.login(nickname, password)
    }

    @GetMapping("/all")
    fun getUsers() : List<User> {
        return userProjection.getAll()
    }

    @GetMapping("/{userId}/projects")
    fun getUserProject(@PathVariable userId: UUID) : UserProjects? {
        return userProjectsProjection.getById(userId)
    }
}