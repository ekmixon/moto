import re
import sys

from moto.dynamodb2.exceptions import (
    InvalidTokenException,
    InvalidExpressionAttributeNameKey,
)


class Token(object):
    _TOKEN_INSTANCE = None
    MINUS_SIGN = "-"
    PLUS_SIGN = "+"
    SPACE_SIGN = " "
    EQUAL_SIGN = "="
    OPEN_ROUND_BRACKET = "("
    CLOSE_ROUND_BRACKET = ")"
    COMMA = ","
    SPACE = " "
    DOT = "."
    OPEN_SQUARE_BRACKET = "["
    CLOSE_SQUARE_BRACKET = "]"

    SPECIAL_CHARACTERS = [
        MINUS_SIGN,
        PLUS_SIGN,
        SPACE_SIGN,
        EQUAL_SIGN,
        OPEN_ROUND_BRACKET,
        CLOSE_ROUND_BRACKET,
        COMMA,
        SPACE,
        DOT,
        OPEN_SQUARE_BRACKET,
        CLOSE_SQUARE_BRACKET,
    ]

    # Attribute: an identifier that is an attribute
    ATTRIBUTE = 0
    # Place holder for attribute name
    ATTRIBUTE_NAME = 1
    # Placeholder for attribute value starts with :
    ATTRIBUTE_VALUE = 2
    # WhiteSpace shall be grouped together
    WHITESPACE = 3
    # Placeholder for a number
    NUMBER = 4

    PLACEHOLDER_NAMES = {
        ATTRIBUTE: "Attribute",
        ATTRIBUTE_NAME: "AttributeName",
        ATTRIBUTE_VALUE: "AttributeValue",
        WHITESPACE: "Whitespace",
        NUMBER: "Number",
    }

    def __init__(self, token_type, value):
        assert (
            token_type in self.SPECIAL_CHARACTERS
            or token_type in self.PLACEHOLDER_NAMES
        )
        self.type = token_type
        self.value = value

    def __repr__(self):
        if isinstance(self.type, int):
            return 'Token("{tt}", "{tv}")'.format(
                tt=self.PLACEHOLDER_NAMES[self.type], tv=self.value
            )
        else:
            return 'Token("{tt}", "{tv}")'.format(tt=self.type, tv=self.value)

    def __eq__(self, other):
        return self.type == other.type and self.value == other.value


class ExpressionTokenizer(object):
    """
    Takes a string and returns a list of tokens. While attribute names in DynamoDB must be between 1 and 255 characters
    long there are no other restrictions for attribute names. For expressions however there are additional rules. If an
    attribute name does not adhere then it must be passed via an ExpressionAttributeName. This tokenizer is aware of the
    rules of Expression attributes.

    We consider a Token as a tuple which has the tokenType

    From https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html
    1) If an attribute name begins with a number or contains a space, a special character, or a reserved word, you
       must use an expression attribute name to replace that attribute's name in the expression.
       => So spaces,+,- or other special characters do identify tokens in update expressions

    2) When using a dot (.) in an attribute name you must use expression-attribute-names. A dot in an expression
       will be interpreted as a separator in a document path

    3) For a nested structure if you want to use expression_attribute_names you must specify one per part of the
       path. Since for members of expression_attribute_names the . is part of the name

    """

    @classmethod
    def is_simple_token_character(cls, character):
        return character.isalnum() or character in ("_", ":", "#")

    @classmethod
    def is_possible_token_boundary(cls, character):
        return (
            character in Token.SPECIAL_CHARACTERS
            or not cls.is_simple_token_character(character)
        )

    @classmethod
    def is_expression_attribute(cls, input_string):
        return re.compile("^[a-zA-Z0-9][a-zA-Z0-9_]*$").match(input_string) is not None

    @classmethod
    def is_expression_attribute_name(cls, input_string):
        """
        https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html
        An expression attribute name must begin with a pound sign (#), and be followed by one or more alphanumeric
         characters.
        """
        return input_string.startswith("#") and cls.is_expression_attribute(
            input_string[1:]
        )

    @classmethod
    def is_expression_attribute_value(cls, input_string):
        return re.compile("^:[a-zA-Z0-9_]*$").match(input_string) is not None

    def raise_unexpected_token(self):
        """If during parsing an unexpected token is encountered"""
        if len(self.token_list) == 0:
            near = ""
        elif len(self.token_list) == 1:
            near = self.token_list[-1].value
        else:
            near = (
                self.token_list[-2].value + self.token_list[-1].value
                if self.token_list[-1].type == Token.WHITESPACE
                else self.token_list[-1].value
            )

        problematic_token = self.staged_characters[0]
        raise InvalidTokenException(problematic_token, near + self.staged_characters)

    def __init__(self, input_expression_str):
        self.input_expression_str = input_expression_str
        self.token_list = []
        self.staged_characters = ""

    @classmethod
    def is_py2(cls):
        return sys.version_info[0] == 2

    @classmethod
    def make_list(cls, input_expression_str):
        if not cls.is_py2():
            assert isinstance(input_expression_str, str)

        return ExpressionTokenizer(input_expression_str)._make_list()

    def add_token(self, token_type, token_value):
        self.token_list.append(Token(token_type, token_value))

    def add_token_from_stage(self, token_type):
        self.add_token(token_type, self.staged_characters)
        self.staged_characters = ""

    @classmethod
    def is_numeric(cls, input_str):
        return re.compile("[0-9]+").match(input_str) is not None

    def process_staged_characters(self):
        if len(self.staged_characters) == 0:
            return
        if self.staged_characters.startswith("#"):
            if self.is_expression_attribute_name(self.staged_characters):
                self.add_token_from_stage(Token.ATTRIBUTE_NAME)
            else:
                raise InvalidExpressionAttributeNameKey(self.staged_characters)
        elif self.is_numeric(self.staged_characters):
            self.add_token_from_stage(Token.NUMBER)
        elif self.is_expression_attribute(self.staged_characters):
            self.add_token_from_stage(Token.ATTRIBUTE)
        elif self.is_expression_attribute_value(self.staged_characters):
            self.add_token_from_stage(Token.ATTRIBUTE_VALUE)
        else:
            self.raise_unexpected_token()

    def _make_list(self):
        """
        Just go through characters if a character is not a token boundary stage it for adding it as a grouped token
        later if it is a tokenboundary process staged characters and then process the token boundary as well.
        """
        for character in self.input_expression_str:
            if not self.is_possible_token_boundary(character):
                self.staged_characters += character
            else:
                self.process_staged_characters()

                if character == Token.SPACE:
                    if (
                        len(self.token_list) > 0
                        and self.token_list[-1].type == Token.WHITESPACE
                    ):
                        self.token_list[-1].value = (
                            self.token_list[-1].value + character
                        )
                    else:
                        self.add_token(Token.WHITESPACE, character)
                elif character in Token.SPECIAL_CHARACTERS:
                    self.add_token(character, character)
                elif not self.is_simple_token_character(character):
                    self.staged_characters += character
                    self.raise_unexpected_token()
                else:
                    raise NotImplementedError(
                        f"Encountered character which was not implemented : {character}"
                    )

        self.process_staged_characters()
        return self.token_list
