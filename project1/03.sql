select Trainer.name from Trainer where Trainer.id not in (select Gym.leader_id from Gym) order by Trainer.name;
