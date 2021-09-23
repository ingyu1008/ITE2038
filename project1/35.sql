select Trainer.name, count(*) as cnt from Trainer join CatchedPokemon on Trainer.id = CatchedPokemon.owner_id group by Trainer.id order by Trainer.name;
