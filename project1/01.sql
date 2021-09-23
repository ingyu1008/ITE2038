select Trainer.name from Trainer join CatchedPokemon on Trainer.id = CatchedPokemon.owner_id group by CatchedPokemon.owner_id having count(*) >= 3 order by count(*);
