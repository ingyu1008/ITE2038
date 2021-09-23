select Trainer.name from Trainer join CatchedPokemon on Trainer.id = CatchedPokemon.owner_id join Pokemon on Pokemon.id = CatchedPokemon.pid where Pokemon.name like 'P%' and Trainer.hometown = 'Sangnok city' order by Trainer.name;
