/*-------------------------------------------------------------------------
 * PostgreSQL Database Management System
 * (formerly known as Postgres, then as Postgres95)
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
 * 
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 * 
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 * isolationtester.h
 *	  include file for isolation tests
 *
 * IDENTIFICATION
 *		src/test/isolation/isolationtester.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ISOLATIONTESTER_H
#define ISOLATIONTESTER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

typedef long int int64;

typedef struct PQExpBufferData
{
	char	   *data;
	size_t		len;
	size_t		maxlen;
} PQExpBufferData;

// typedef PQExpBufferData *PQExpBuffer;

/*
 * The structs declared here are used in the output of specparse.y.
 * Except where noted, all fields are set in the grammar and not
 * changed thereafter.
 */
typedef struct Step Step;

typedef struct
{
	char	   *name;
	char	   *setupsql;
	char	   *teardownsql;
	Step	  **steps;
	int			nsteps;
} Session;

struct Step
{
	char	   *name;
	char	   *sql;
	/* These fields are filled by check_testspec(): */
	int			session;		/* identifies owning session */
	bool		used;			/* has step been used in a permutation? */
};

typedef enum
{
	PSB_ONCE,					/* force step to wait once */
	PSB_OTHER_STEP,				/* wait for another step to complete first */
	PSB_NUM_NOTICES				/* wait for N notices from another session */
} PermutationStepBlockerType;

typedef struct
{
	char	   *stepname;
	PermutationStepBlockerType blocktype;
	int			num_notices;	/* only used for PSB_NUM_NOTICES */
	/* These fields are filled by check_testspec(): */
	Step	   *step;			/* link to referenced step (if any) */
	/* These fields are runtime workspace: */
	int			target_notices; /* total notices needed from other session */
} PermutationStepBlocker;

typedef struct
{
	char	   *name;			/* name of referenced Step */
	PermutationStepBlocker **blockers;
	int			nblockers;
	/* These fields are filled by check_testspec(): */
	Step	   *step;			/* link to referenced Step */
} PermutationStep;

typedef struct
{
	int			nsteps;
	PermutationStep **steps;
} Permutation;

typedef struct
{
	char	  **setupsqls;
	int			nsetupsqls;
	char	   *teardownsql;
	Session   **sessions;
	int			nsessions;
	Permutation **permutations;
	int			npermutations;
} TestSpec;

extern TestSpec parseresult;

extern int	spec_yyparse(void);

extern int	spec_yylex(void);
extern void spec_yyerror(const char *message);

#endif							/* ISOLATIONTESTER_H */
