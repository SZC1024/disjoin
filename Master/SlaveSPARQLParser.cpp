#include "SlaveSPARQLParser.h"
#include "SlaveSPARQLLexer.h"
#include <cstdlib>
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
using namespace std;

//---------------------------------------------------------------------------
SlaveSPARQLParser::ParserException::ParserException(const string &message)
		: message(message)
// Constructor
{
}

//---------------------------------------------------------------------------
SlaveSPARQLParser::ParserException::ParserException(const char *message)
		: message(message)
// Constructor
{
}

//---------------------------------------------------------------------------
SlaveSPARQLParser::ParserException::~ParserException()
// Destructor
{
}

//---------------------------------------------------------------------------
SlaveSPARQLParser::Pattern::Pattern(Element subject, Element predicate, Element object)
		: subject(subject), predicate(predicate), object(object)
// Constructor
{
}

//---------------------------------------------------------------------------
SlaveSPARQLParser::Pattern::~Pattern()
// Destructor
{
}

//---------------------------------------------------------------------------
SlaveSPARQLParser::SlaveSPARQLParser(SlaveSPARQLLexer &lexer)
		: lexer(lexer), variableCount(1), projectionModifier(Modifier_None), limit(~0u)
// Constructor
{
}

//---------------------------------------------------------------------------
SlaveSPARQLParser::~SlaveSPARQLParser()
// Destructor
{
}

//---------------------------------------------------------------------------
unsigned SlaveSPARQLParser::nameVariable(const string &name)
// Lookup or create a named variable
{
	if (namedVariables.count(name))
		return namedVariables[name];
	
	unsigned result = variableCount++;
	namedVariables[name] = result;
	return result;
}

void PrintPatterns(const SlaveSPARQLParser::PatternGroup &group) {
	std::vector<SlaveSPARQLParser::Pattern>::const_iterator iter = group.patterns.begin();
	std::vector<SlaveSPARQLParser::Pattern>::const_iterator limit = group.patterns.end();
	
	for (; iter != limit; ++iter) {
		if ((*iter).subject.type == SlaveSPARQLParser::Element::IRI)
			std::cout << (*iter).subject.value << "  ";
		else if ((*iter).subject.type == SlaveSPARQLParser::Element::Variable)
			std::cout << "Variable" << "  ";
		if ((*iter).predicate.type == SlaveSPARQLParser::Element::IRI)
			std::cout << (*iter).predicate.value << "  ";
		else if ((*iter).predicate.type == SlaveSPARQLParser::Element::Variable)
			std::cout << "Variable" << "  ";
		if ((*iter).object.type == SlaveSPARQLParser::Element::IRI)
			std::cout << (*iter).object.value << std::endl;
		else if ((*iter).object.type == SlaveSPARQLParser::Element::Variable)
			std::cout << "Variable" << std::endl;
	}
}

//---------------------------------------------------------------------------
void SlaveSPARQLParser::parsePrefix()
// Parse the prefix part if any
{
	while (true) {
		SlaveSPARQLLexer::Token token = lexer.getNext();
		
		if ((token == SlaveSPARQLLexer::Identifier) && (lexer.isKeyword("prefix"))) {
			// Parse the prefix entry
			if (lexer.getNext() != SlaveSPARQLLexer::Identifier)
				throw ParserException("prefix name expected");
			string name = lexer.getTokenValue();
			if (lexer.getNext() != SlaveSPARQLLexer::Colon)
				throw ParserException("':' expected");
			if (lexer.getNext() != SlaveSPARQLLexer::IRI)
				throw ParserException("IRI expected");
			string iri = lexer.getTokenValue();
			
			// Register the new prefix
			if (prefixes.count(name))
				throw ParserException("duplicate prefix '" + name + "'");
			prefixes[name] = iri;
		} else {
			lexer.unget(token);
			return;
		}
	}
}

//--------------------------------------------------------------------------
void SlaveSPARQLParser::parseQueryString()
// Parse the QueryString
{
	if ((lexer.getNext() == SlaveSPARQLLexer::Identifier)) {
		if (lexer.isKeyword("select")) {
			parseQuery();
		} else if (lexer.isKeyword("insert")) {
//			std::cout << "Insert data" << std::endl;
			parseInsert();
		} else if (lexer.isKeyword("delete")) {
			parseDelete();
		} else {
			throw ParserException("'select' or 'insert' or 'delete' expected");
		}
	} else {
		throw ParserException("'select' or 'insert' or 'delete' expected");
	}
}

//---------------------------------------------------------
void SlaveSPARQLParser::parseQuery() {
	
	QueryOperation = SlaveSPARQLParser::QUERY;
	
	// Parse the projection
	parseProjection();
	
	// Parse the from claus
	parseFrom();
	
	// Parse the where clause
	parseWhere();
	
	// Parse the limit clause
	parseLimit();
	
	// Check that the input is done
	if (lexer.getNext() != SlaveSPARQLLexer::Eof)
		throw ParserException("syntax error");
	
	// Fixup empty projections (i.e. *)
	if (!projection.size()) {
		for (map<string, unsigned>::const_iterator iter = namedVariables.begin(), limit = namedVariables.end();
		     iter != limit; ++iter)
			projection.push_back((*iter).second);
	}
}

//---------------------------------------------------------------------------
void SlaveSPARQLParser::parseInsert()
// Parse the Insert
{
	QueryOperation = SlaveSPARQLParser::INSERT_DATA;
	
	if ((lexer.getNext() != SlaveSPARQLLexer::Identifier) || (!lexer.isKeyword("data")))
		throw ParserException(" 'data' expected");
	
	if (lexer.getNext() != SlaveSPARQLLexer::LCurly)
		throw ParserException(" '{' expected");

//	std::cout << "before parseGroupGrappattern" << std::endl;
	
	patterns = PatternGroup();
	parseGroupGraphPattern(patterns);
	
	if (lexer.getNext() != SlaveSPARQLLexer::Eof)
		throw ParserException(" syntax error ");


//	PrintPatterns(patterns);

}

//--------------------------------------------------------------------------
void SlaveSPARQLParser::parseDelete()
// Parse the Delete
{
	SlaveSPARQLLexer::Token token = lexer.getNext();
	
	if ((token == SlaveSPARQLLexer::Identifier) && (lexer.isKeyword("data"))) {
		parseDeleteData();
	} else if (token == SlaveSPARQLLexer::LCurly) {
		parseDeleteClause();
	} else {
		throw ParserException("delete syntax error");
	}
}

//---------------------------------------------------------------------------
void SlaveSPARQLParser::parseDeleteData()
// Parse the delete data
{
	QueryOperation = SlaveSPARQLParser::DELETE_DATA;
	
	if (lexer.getNext() != SlaveSPARQLLexer::LCurly)
		throw ParserException(" '{' expected");
	
	patterns = PatternGroup();
	parseGroupGraphPattern(patterns);
	
	if (lexer.getNext() != SlaveSPARQLLexer::Eof)
		throw ParserException(" syntax error");

//	PrintPatterns(patterns);
}

//--------------------------------------------------------------------------
void SlaveSPARQLParser::parseDeleteClause()
// Parse the Delete Clause
{
	patterns = PatternGroup();
	parseGroupGraphPattern(patterns);
	
	SlaveSPARQLLexer::Token token = lexer.getNext();
	
	if (token == SlaveSPARQLLexer::Eof) {
		QueryOperation = SlaveSPARQLParser::DELETE_CLAUSE;
//		PrintPatterns(patterns);
	} else if ((token == SlaveSPARQLLexer::Identifier) && (lexer.isKeyword("insert"))) {
		parseUpdate();
	} else {
		throw ParserException("syntax error");
	}
}

//----------------------------------------------------------------------
void SlaveSPARQLParser::parseUpdate()
// Parse the Update
{
	QueryOperation = SlaveSPARQLParser::UPDATE;
	if (lexer.getNext() != SlaveSPARQLLexer::LCurly)
		throw ParserException(" '{' expected");
	
	parseGroupGraphPattern(patterns);
	
	if (lexer.getNext() != SlaveSPARQLLexer::Eof) {
		throw ParserException(" syntax error ");
	}

//	PrintPatterns(patterns);
}

//---------------------------------------------------------------------------
void SlaveSPARQLParser::parseProjection()
// Parse the projection
{
//   // Parse the projection
//   if ((lexer.getNext()!=SlaveSPARQLLexer::Identifier)||(!lexer.isKeyword("select")))
//      throw ParserException("'select' expected");
	
	// Parse modifiers, if any
	{
		SlaveSPARQLLexer::Token token = lexer.getNext();
		if (token == SlaveSPARQLLexer::Identifier) {
			if (lexer.isKeyword("distinct")) projectionModifier = Modifier_Distinct;
			else if (lexer.isKeyword("reduced")) projectionModifier = Modifier_Reduced;
			else if (lexer.isKeyword("count")) projectionModifier = Modifier_Count;
			else if (lexer.isKeyword("duplicates")) projectionModifier = Modifier_Duplicates;
			else
				lexer.unget(token);
		} else lexer.unget(token);
	}
	
	// Parse the projection clause
	bool first = true;
	while (true) {
		SlaveSPARQLLexer::Token token = lexer.getNext();
		if (token == SlaveSPARQLLexer::Variable) {
			projection.push_back(nameVariable(lexer.getTokenValue()));
		} else if (token == SlaveSPARQLLexer::Star) {
			// We do nothing here. Empty projections will be filled with all
			// named variables after parsing
		} else {
			if (first)
				throw ParserException("projection required after select");
			lexer.unget(token);
			break;
		}
		first = false;
	}
}

//---------------------------------------------------------------------------
void SlaveSPARQLParser::parseFrom()
// Parse the from part if any
{
	while (true) {
		SlaveSPARQLLexer::Token token = lexer.getNext();
		
		if ((token == SlaveSPARQLLexer::Identifier) && (lexer.isKeyword("from"))) {
			throw ParserException("from clause currently not implemented");
		} else {
			lexer.unget(token);
			return;
		}
	}
}

//---------------------------------------------------------------------------
void SlaveSPARQLParser::parseFilter(PatternGroup &group, map<string, unsigned> &localVars)
// Parse a filter condition
{
	// '('
	if (lexer.getNext() != SlaveSPARQLLexer::LParen)
		throw ParserException("'(' expected");
	
	// Variable
	Element var = parsePatternElement(group, localVars);
	if (var.type != Element::Variable)
		throw ParserException("filter variable expected");
	
	// Prepare the setuo
	vector <Element> values;
	Filter::Type type;
	
	// 'in'?
	SlaveSPARQLLexer::Token token = lexer.getNext();
	if ((token == SlaveSPARQLLexer::Identifier) && (lexer.isKeyword("in"))) {
		// The values
		while (true) {
			Element e = parsePatternElement(group, localVars);
			if (e.type == Element::Variable)
				throw ParserException("constant values required in 'in' filter");
			values.push_back(e);
			
			SlaveSPARQLLexer::Token token = lexer.getNext();
			if (token == SlaveSPARQLLexer::Comma)
				continue;
			if (token == SlaveSPARQLLexer::RParen)
				break;
			throw ParserException("',' or ')' expected");
		}
		type = Filter::Normal;
	} else if ((token == SlaveSPARQLLexer::Identifier) && (lexer.isKeyword("reaches"))) {
		Element target = parsePatternElement(group, localVars);
		if (target.type == Element::Variable)
			throw ParserException("constant values required in 'reaches' filter");
		
		token = lexer.getNext();
		if ((token != SlaveSPARQLLexer::Identifier) || (!lexer.isKeyword("via")))
			throw ParserException("'via' expected");
		
		Element path = parsePatternElement(group, localVars);
		if (target.type == Element::Variable)
			throw ParserException("constant values required in 'reaches' filter");
		
		values.push_back(target);
		values.push_back(path);
		type = Filter::Path;
		
		if (lexer.getNext() != SlaveSPARQLLexer::RParen)
			throw ParserException("')' expected");
	} else if ((token == SlaveSPARQLLexer::Equal) || (token == SlaveSPARQLLexer::NotEqual)) {
		Element e = parsePatternElement(group, localVars);
		values.push_back(e);
		if (lexer.getNext() != SlaveSPARQLLexer::RParen)
			throw ParserException("')' expected");
		type = (token == SlaveSPARQLLexer::Equal) ? Filter::Normal : Filter::Exclude;
	} else throw ParserException("'=', '!=', 'in', or 'reachable' expected");
	
	// Remember the filter
	Filter f;
	f.id = var.id;
	f.values = values;
	f.type = type;
	group.filters.push_back(f);
}

//---------------------------------------------------------------------------
SlaveSPARQLParser::Element SlaveSPARQLParser::parseBlankNode(PatternGroup &group, map<string, unsigned> &localVars)
// Parse blank node patterns
{
	// The subject is a blank node
	Element subject;
	subject.type = Element::Variable;
	subject.id = variableCount++;
	
	// Parse the the remaining part of the pattern
	SlaveSPARQLParser::Element predicate = parsePatternElement(group, localVars);
	SlaveSPARQLParser::Element object = parsePatternElement(group, localVars);
	group.patterns.push_back(Pattern(subject, predicate, object));
	
	// Check for the tail
	while (true) {
		SlaveSPARQLLexer::Token token = lexer.getNext();
		if (token == SlaveSPARQLLexer::Semicolon) {
			predicate = parsePatternElement(group, localVars);
			object = parsePatternElement(group, localVars);
			group.patterns.push_back(Pattern(subject, predicate, object));
			continue;
		} else if (token == SlaveSPARQLLexer::Comma) {
			object = parsePatternElement(group, localVars);
			group.patterns.push_back(Pattern(subject, predicate, object));
			continue;
		} else if (token == SlaveSPARQLLexer::Dot) {
			return subject;
		} else if (token == SlaveSPARQLLexer::RBracket) {
			lexer.unget(token);
			return subject;
		} else if (token == SlaveSPARQLLexer::Identifier) {
			if (!lexer.isKeyword("filter"))
				throw ParserException("'filter' expected");
			parseFilter(group, localVars);
			continue;
		} else {
			// Error while parsing, let out caller handle it
			lexer.unget(token);
			return subject;
		}
	}
}

//---------------------------------------------------------------------------
SlaveSPARQLParser::Element SlaveSPARQLParser::parsePatternElement(PatternGroup &group, map<string, unsigned> &localVars)
// Parse an entry in a pattern
{
	Element result;
	SlaveSPARQLLexer::Token token = lexer.getNext();
	if (token == SlaveSPARQLLexer::Variable) {
		result.type = Element::Variable;
		result.id = nameVariable(lexer.getTokenValue());
	} else if (token == SlaveSPARQLLexer::Anon) {
		result.type = Element::Variable;
		result.id = variableCount++;
	} else if (token == SlaveSPARQLLexer::LBracket) {
		result = parseBlankNode(group, localVars);
		if (lexer.getNext() != SlaveSPARQLLexer::RBracket)
			throw ParserException("']' expected");
	} else if (token == SlaveSPARQLLexer::Underscore) {
		// _:variable
		if (lexer.getNext() != SlaveSPARQLLexer::Colon)
			throw ParserException("':' expected");
		if (lexer.getNext() != SlaveSPARQLLexer::Identifier)
			throw ParserException("identifier expected after ':'");
		result.type = Element::Variable;
		if (localVars.count(lexer.getTokenValue()))
			result.id = localVars[lexer.getTokenValue()];
		else
			result.id = localVars[lexer.getTokenValue()] = variableCount++;
	} else if (token == SlaveSPARQLLexer::Colon) {
		// :identifier. Should reference the base
		if (lexer.getNext() != SlaveSPARQLLexer::Identifier)
			throw ParserException("identifier expected after ':'");
		result.type = Element::IRI;
		result.value = lexer.getTokenValue();
	} else if (token == SlaveSPARQLLexer::Identifier) {
		result.type = Element::IRI;
		result.value = lexer.getTokenValue();
	} else {
		throw ParserException("invalid pattern element");
	}
	return result;
}

//---------------------------------------------------------------------------
void SlaveSPARQLParser::parseGraphPattern(PatternGroup &group)
// Parse a graph pattern
{
	map<string, unsigned> localVars;
	
	// Parse the first pattern
	Element subject = parsePatternElement(group, localVars);
	Element predicate = parsePatternElement(group, localVars);
	Element object = parsePatternElement(group, localVars);
	group.patterns.push_back(Pattern(subject, predicate, object));

//#define PARSER_DEBUG
#ifdef PARSER_DEBUG
	//打印主语
	if (subject.type == Element::Variable)
		std::cout << "subject is variable, id=" << subject.id << std::endl;
	else
		std::cout << "subject is constant, value=" << subject.value << endl;
	
	//打印谓词
	if (predicate.type == Element::Variable)
		std::cout << "predicate is variable, id=" << predicate.id << std::endl;
	else
		std::cout << "predicate is constant, value=" << predicate.value << endl;
	
	//打印宾语
	if (object.type == Element::Variable)
		std::cout << "object is variable, id=" << object.id << std::endl;
	else
		std::cout << "object is constant, value=" << object.value << std::endl;
#endif
	
	// Check for the tail
	while (true) {
		SlaveSPARQLLexer::Token token = lexer.getNext();
		if (token == SlaveSPARQLLexer::Semicolon) {
			predicate = parsePatternElement(group, localVars);
			object = parsePatternElement(group, localVars);
			group.patterns.push_back(Pattern(subject, predicate, object));
			continue;
		} else if (token == SlaveSPARQLLexer::Comma) {
			object = parsePatternElement(group, localVars);
			group.patterns.push_back(Pattern(subject, predicate, object));
			continue;
		} else if (token == SlaveSPARQLLexer::Dot) {
			return;
		} else if (token == SlaveSPARQLLexer::RCurly) {
			lexer.unget(token);
			return;
		} else if (token == SlaveSPARQLLexer::Identifier) {
			if (!lexer.isKeyword("filter"))
				throw ParserException("'filter' expected");
			parseFilter(group, localVars);
			continue;
		} else {
			// Error while parsing, let our caller handle it
			lexer.unget(token);
			return;
		}
	}
}

//---------------------------------------------------------------------------
void SlaveSPARQLParser::parseGroupGraphPattern(PatternGroup &group)
// Parse a group of patterns
{
	while (true) {
		SlaveSPARQLLexer::Token token = lexer.getNext();
		
		if (token == SlaveSPARQLLexer::LCurly) {
			// Parse the group
			PatternGroup newGroup;
			parseGroupGraphPattern(newGroup);
			
			// Union statement?
			token = lexer.getNext();
			if ((token == SlaveSPARQLLexer::Identifier) && (lexer.isKeyword("union"))) {
				group.unions.push_back(vector<PatternGroup>());
				vector <PatternGroup> &currentUnion = group.unions.back();
				currentUnion.push_back(newGroup);
				while (true) {
					if (lexer.getNext() != SlaveSPARQLLexer::LCurly)
						throw ParserException("'{' expected");
					PatternGroup subGroup;
					parseGroupGraphPattern(subGroup);
					currentUnion.push_back(subGroup);
					
					// Another union?
					token = lexer.getNext();
					if ((token == SlaveSPARQLLexer::Identifier) && (lexer.isKeyword("union")))
						continue;
					break;
				}
			} else {
				// No, simply merge it
				group.patterns.insert(group.patterns.end(), newGroup.patterns.begin(), newGroup.patterns.end());
				group.filters.insert(group.filters.end(), newGroup.filters.begin(), newGroup.filters.end());
				group.optional.insert(group.optional.end(), newGroup.optional.begin(), newGroup.optional.end());
				group.unions.insert(group.unions.end(), newGroup.unions.begin(), newGroup.unions.end());
			}
			if (token != SlaveSPARQLLexer::Dot)
				lexer.unget(token);
		} else if ((token == SlaveSPARQLLexer::IRI) || (token == SlaveSPARQLLexer::Variable) ||
		           (token == SlaveSPARQLLexer::Identifier) || (token == SlaveSPARQLLexer::String) ||
		           (token == SlaveSPARQLLexer::Underscore) || (token == SlaveSPARQLLexer::Colon) ||
		           (token == SlaveSPARQLLexer::LBracket) || (token == SlaveSPARQLLexer::Anon)) {
			// Distinguish filter conditions
			if ((token == SlaveSPARQLLexer::Identifier) && (lexer.isKeyword("filter"))) {
				map<string, unsigned> localVars;
				parseFilter(group, localVars);
			} else {
				lexer.unget(token);
				parseGraphPattern(group);
			}
		} else if (token == SlaveSPARQLLexer::RCurly) {
			break;
		} else {
			throw ParserException("'}' expected");
		}
	}
}

//---------------------------------------------------------------------------
void SlaveSPARQLParser::parseWhere()
// Parse the where part if any
{
	while (true) {
		SlaveSPARQLLexer::Token token = lexer.getNext();
		
		if ((token == SlaveSPARQLLexer::Identifier) && (lexer.isKeyword("where"))) {
			if (lexer.getNext() != SlaveSPARQLLexer::LCurly)
				throw ParserException("'{' expected");
			
			patterns = PatternGroup();
			parseGroupGraphPattern(patterns);
		} else {
			lexer.unget(token);
			return;
		}
	}
}

//---------------------------------------------------------------------------
void SlaveSPARQLParser::parseLimit()
// Parse the limit part if any
{
	SlaveSPARQLLexer::Token token = lexer.getNext();
	
	if ((token == SlaveSPARQLLexer::Identifier) && (lexer.isKeyword("limit"))) {
		if (lexer.getNext() != SlaveSPARQLLexer::Identifier)
			throw ParserException("number expected after 'limit'");
		limit = atoi(lexer.getTokenValue().c_str());
		if (limit == 0)
			throw ParserException("invalid limit specifier");
	} else {
		lexer.unget(token);
	}
}

//---------------------------------------------------------------------------
void SlaveSPARQLParser::parse()
// Parse the input
{
	// Parse the prefix part
	parsePrefix();
	
	// Parser the QueryString
	parseQueryString();
//   // Parse the projection
//   parseProjection();
//
//   // Parse the from clause
//   parseFrom();
//
//   // Parse the where clause
//   parseWhere();
//
//   // Parse the limit clause
//   parseLimit();
//
//   // Check that the input is done
//   if (lexer.getNext()!=SPARQLLexer::Eof)
//      throw ParserException("syntax error");
//
//   // Fixup empty projections (i.e. *)
//   if (!projection.size()) {
//      for (map<string,unsigned>::const_iterator iter=namedVariables.begin(),limit=namedVariables.end();iter!=limit;++iter)
//         projection.push_back((*iter).second);
//   }
}
//---------------------------------------------------------------------------
