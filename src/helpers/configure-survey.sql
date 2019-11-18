select b.name,b.repeat,q.id,q.name,q.type
from frm_questions as q
	inner join frm_blocks as b on q.block = b.id;