select Trainer.name from Trainer join Gym on Trainer.id = Gym.leader_id where Gym.city in (select City.name from City where City.description = 'Amazon');
